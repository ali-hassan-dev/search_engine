import os
import logging
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import time

import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import unicodedata
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class JobOffer:
    """Data class for job offers"""

    id: int
    intitule: str
    description: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DatabaseManager:
    """Handles MySQL database connections and operations"""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.config = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "charset": "utf8mb4",
            "collation": "utf8mb4_unicode_ci",
            "autocommit": True,
        }

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = mysql.connector.connect(**self.config)
            yield conn
        except mysql.connector.Error as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

    def fetch_all_jobs(self, batch_size: int = 1000):
        """Yield JobOffer in batches by paginating on the primary key."""
        last_id = 0
        query = """
            SELECT id, intitule, description
            FROM PE_offres
            WHERE id > %s
                AND intitule IS NOT NULL
                AND description IS NOT NULL
            ORDER BY id
            LIMIT %s
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            while True:
                cursor.execute(query, (last_id, batch_size))
                rows = cursor.fetchall()
                if not rows:
                    break
                for row in rows:
                    yield JobOffer(
                        id=row["id"],
                        intitule=row["intitule"] or "",
                        description=row["description"] or "",
                    )
                last_id = rows[-1]["id"]
            cursor.close()

    # def fetch_all_jobs(self) -> List[JobOffer]:
    #     """Fetch all job offers from MySQL"""
    #     with self.get_connection() as conn:
    #         cursor = conn.cursor(dictionary=True)
    #         cursor.execute("""
    #             SELECT id, intitule, description
    #             FROM PE_offres
    #             WHERE intitule IS NOT NULL
    #             AND description IS NOT NULL
    #         """)

    #         jobs = []
    #         for row in cursor.fetchall():
    #             jobs.append(JobOffer(
    #                 id=row['id'],
    #                 intitule=row['intitule'] or '',
    #                 description=row['description'] or ''
    #             ))

    #         cursor.close()
    #         return jobs


class SearchEngine:
    """Elasticsearch-based search engine for job offers"""

    INDEX_NAME = "job_offers"

    def __init__(self, es_host: str = "localhost", es_port: int = 9200):
        self.es = Elasticsearch([{"host": es_host, "port": es_port}])
        self._create_index()

    def _create_index(self):
        """Create Elasticsearch index with French analysis"""

        if self.es.indices.exists(index=self.INDEX_NAME):
            self.es.indices.delete(index=self.INDEX_NAME)

        index_config = {
            "settings": {
                "analysis": {
                    "filter": {
                        "french_elision": {
                            "type": "elision",
                            "articles_case": True,
                            "articles": [
                                "l",
                                "m",
                                "t",
                                "qu",
                                "n",
                                "s",
                                "j",
                                "d",
                                "c",
                                "jusqu",
                                "quoiqu",
                                "lorsqu",
                                "puisqu",
                            ],
                        },
                        "french_stemmer": {
                            "type": "stemmer",
                            "language": "light_french",
                        },
                        "french_stop": {"type": "stop", "stopwords": "_french_"},
                        "compound_word_filter": {
                            "type": "word_delimiter_graph",
                            "generate_word_parts": True,
                            "generate_number_parts": True,
                            "catenate_words": True,
                            "catenate_numbers": True,
                            "catenate_all": True,
                            "split_on_case_change": True,
                            "split_on_numerics": True,
                            "preserve_original": True,
                        },
                    },
                    "analyzer": {
                        "french_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "french_elision",
                                "lowercase",
                                "french_stop",
                                "french_stemmer",
                                "compound_word_filter",
                            ],
                        },
                        "search_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "french_elision",
                                "lowercase",
                                "compound_word_filter",
                                "french_stemmer",
                            ],
                        },
                    },
                },
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
            "mappings": {
                "properties": {
                    "id": {"type": "integer"},
                    "intitule": {
                        "type": "text",
                        "analyzer": "french_analyzer",
                        "search_analyzer": "search_analyzer",
                        "fields": {
                            "exact": {"type": "keyword"},
                            "suggest": {
                                "type": "completion",
                                "analyzer": "french_analyzer",
                            },
                        },
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "french_analyzer",
                        "search_analyzer": "search_analyzer",
                    },
                    "combined_text": {
                        "type": "text",
                        "analyzer": "french_analyzer",
                        "search_analyzer": "search_analyzer",
                    },
                }
            },
        }

        self.es.indices.create(index=self.INDEX_NAME, body=index_config)
        logger.info(f"Created index: {self.INDEX_NAME}")

    def index_jobs(self, jobs: List[JobOffer]):
        """Index job offers in Elasticsearch"""

        def doc_generator():
            for job in jobs:
                combined_text = f"{job.intitule} {job.description}"

                doc = {
                    "_index": self.INDEX_NAME,
                    "_id": job.id,
                    "_source": {
                        "id": job.id,
                        "intitule": job.intitule,
                        "description": job.description,
                        "combined_text": combined_text,
                    },
                }
                yield doc

        success, failed = bulk(self.es, doc_generator(), chunk_size=1000)
        logger.info(f"Indexed {success} documents, {len(failed)} failed")

    def search(self, query: str, size: int = 50, from_: int = 0) -> Dict[str, Any]:
        """Search for job offers"""

        if not query.strip():
            return {"hits": {"total": {"value": 0}, "hits": []}, "took": 0}

        normalized_query = self._normalize_query(query)

        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match_phrase": {
                                "intitule": {"query": normalized_query, "boost": 3.0}
                            }
                        },
                        {
                            "match_phrase": {
                                "description": {"query": normalized_query, "boost": 2.0}
                            }
                        },
                        {
                            "match": {
                                "intitule": {
                                    "query": normalized_query,
                                    "boost": 2.5,
                                    "fuzziness": "AUTO",
                                    "operator": "and",
                                }
                            }
                        },
                        {
                            "match": {
                                "description": {
                                    "query": normalized_query,
                                    "boost": 1.5,
                                    "fuzziness": "AUTO",
                                    "operator": "and",
                                }
                            }
                        },
                        {
                            "match": {
                                "combined_text": {
                                    "query": normalized_query,
                                    "boost": 1.0,
                                    "fuzziness": "AUTO",
                                }
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
            "highlight": {
                "fields": {
                    "intitule": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                    "description": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                }
            },
            "size": size,
            "from": from_,
        }

        try:
            response = self.es.search(index=self.INDEX_NAME, body=search_body)
            return response
        except Exception as e:
            logger.error(f"Search error: {e}")
            return {"hits": {"total": {"value": 0}, "hits": []}, "took": 0}

    def _normalize_query(self, query: str) -> str:
        """Normalize search query"""
        query = unicodedata.normalize("NFD", query)
        query = "".join(c for c in query if unicodedata.category(c) != "Mn")

        query = query.lower().strip()

        query = re.sub(
            r"\b(no)\s*[-\s]*\s*(code)\b", r'nocode no-code "no code"', query
        )

        return query


class SearchAPI:
    """Flask API for job search"""

    def __init__(self, search_engine: SearchEngine):
        self.search_engine = search_engine
        self.app = Flask(__name__)
        CORS(self.app)
        self._setup_routes()

    def _setup_routes(self):
        """Setup Flask routes"""

        @self.app.route("/")
        def index():
            return render_template_string(self._get_frontend_template())

        @self.app.route("/search", methods=["GET"])
        def search():
            query = request.args.get("q", "").strip()
            size = min(int(request.args.get("size", 50)), 100)
            from_ = int(request.args.get("from", 0))

            if not query:
                return jsonify({"query": query, "total": 0, "results": [], "took": 0})

            start_time = time.time()
            response = self.search_engine.search(query, size, from_)
            search_time = int((time.time() - start_time) * 1000)

            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                highlight = hit.get("highlight", {})

                result = {
                    "id": source["id"],
                    "intitule": source["intitule"],
                    "description": (
                        source["description"][:500] + "..."
                        if len(source["description"]) > 500
                        else source["description"]
                    ),
                    "score": hit["_score"],
                    "highlight": {
                        "intitule": highlight.get("intitule", []),
                        "description": highlight.get("description", []),
                    },
                }
                results.append(result)

            return jsonify(
                {
                    "query": query,
                    "total": response["hits"]["total"]["value"],
                    "results": results,
                    "took": search_time,
                    "elasticsearch_took": response["took"],
                }
            )

        @self.app.errorhandler(Exception)
        def handle_error(e):
            logger.error(f"API Error: {e}")
            return jsonify({"error": "Internal server error"}), 500

    def _get_frontend_template(self):
        """Simple frontend template for testing"""
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Job Search Engine</title>
            <meta charset="UTF-8">
            <style>
                body { font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }
                .search-container { margin-bottom: 20px; }
                .search-box { width: 70%; padding: 10px; font-size: 16px; border: 1px solid #ddd; border-radius: 4px; }
                .search-btn { padding: 10px 20px; font-size: 16px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; }
                .results { margin-top: 20px; }
                .result-item { border: 1px solid #ddd; padding: 15px; margin-bottom: 10px; border-radius: 4px; }
                .result-title { font-weight: bold; color: #007bff; margin-bottom: 5px; }
                .result-description { color: #666; line-height: 1.4; }
                .result-meta { font-size: 12px; color: #999; margin-top: 5px; }
                .stats { margin-bottom: 10px; color: #666; }
                .loading { text-align: center; padding: 20px; color: #666; }
                mark { background-color: #ffeb3b; padding: 1px 2px; }
            </style>
        </head>
        <body>
            <h1>Job Search Engine</h1>
            <div class="search-container">
                <input type="text" id="searchBox" class="search-box" placeholder="Rechercher des offres d'emploi (ex: développeur, drone, no-code)..." />
                <button onclick="search()" class="search-btn">Rechercher</button>
            </div>
            <div id="stats" class="stats"></div>
            <div id="results" class="results"></div>
            
            <script>
                let searchTimeout;
                
                document.getElementById('searchBox').addEventListener('input', function() {
                    clearTimeout(searchTimeout);
                    searchTimeout = setTimeout(search, 300);
                });
                
                document.getElementById('searchBox').addEventListener('keypress', function(e) {
                    if (e.key === 'Enter') {
                        search();
                    }
                });
                
                function search() {
                    const query = document.getElementById('searchBox').value.trim();
                    const resultsDiv = document.getElementById('results');
                    const statsDiv = document.getElementById('stats');
                    
                    if (!query) {
                        resultsDiv.innerHTML = '';
                        statsDiv.innerHTML = '';
                        return;
                    }
                    
                    resultsDiv.innerHTML = '<div class="loading">Recherche en cours...</div>';
                    statsDiv.innerHTML = '';
                    
                    fetch(`/search?q=${encodeURIComponent(query)}`)
                        .then(response => response.json())
                        .then(data => {
                            statsDiv.innerHTML = `${data.total} résultats trouvés en ${data.took}ms`;
                            
                            if (data.results.length === 0) {
                                resultsDiv.innerHTML = '<div>Aucun résultat trouvé.</div>';
                                return;
                            }
                            
                            let html = '';
                            data.results.forEach(result => {
                                const title = result.highlight.intitule.length > 0 ? result.highlight.intitule[0] : result.intitule;
                                const description = result.highlight.description.length > 0 ? result.highlight.description[0] : result.description;
                                
                                html += `
                                    <div class="result-item">
                                        <div class="result-title">${title}</div>
                                        <div class="result-description">${description}</div>
                                        <div class="result-meta">Score: ${result.score.toFixed(2)} | ID: ${result.id}</div>
                                    </div>
                                `;
                            });
                            
                            resultsDiv.innerHTML = html;
                        })
                        .catch(error => {
                            console.error('Search error:', error);
                            resultsDiv.innerHTML = '<div>Erreur lors de la recherche.</div>';
                        });
                }
            </script>
        </body>
        </html>
        """

    def run(self, host: str = "127.0.0.1", port: int = 5000, debug: bool = False):
        """Run the Flask application"""
        self.app.run(host=host, port=port, debug=debug)


def main():
    """Main function to setup and run the search engine"""

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT"))
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    ES_HOST = os.getenv("ES_HOST", "localhost")
    ES_PORT = int(os.getenv("ES_PORT", 9200))

    try:
        logger.info("Initializing search engine...")

        db_manager = DatabaseManager(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
        search_engine = SearchEngine(ES_HOST, ES_PORT)

        logger.info("Loading job offers from MySQL...")
        # jobs = db_manager.fetch_all_jobs()
        jobs_gen = db_manager.fetch_all_jobs(batch_size=1000)
        # logger.info(f"Loaded {len(jobs)} job offers")

        logger.info("Indexing jobs in Elasticsearch...")
        search_engine.index_jobs(jobs_gen)
        # search_engine.index_jobs(jobs)

        logger.info("Starting search API server...")
        api = SearchAPI(search_engine)
        api.run(host="0.0.0.0", port=5000, debug=True)

    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


if __name__ == "__main__":
    main()
