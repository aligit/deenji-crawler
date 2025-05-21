# es_indexer.py

import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

from text_utils import convert_to_persian_digits, generate_bedroom_variants

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:
    # Fallback definition if package is not available
    def load_dotenv():
        import os

        print("Warning: python-dotenv package not found, using fallback implementation")
        # Read .env file manually if it exists
        if os.path.exists(".env"):
            with open(".env") as f:
                for line in f:
                    if line.strip() and not line.startswith("#"):
                        key, value = line.strip().split("=", 1)
                        os.environ[key] = value


from elasticsearch import Elasticsearch

load_dotenv()


class DivarElasticsearchIndexer:
    def __init__(self):
        self.es_host = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
        self.property_index = "divar_properties"
        self.suggestion_index = "divar_suggestions"
        self.es = None
        self.headers = {
            "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
            "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
        }

    async def init_client(self):
        """Initialize Elasticsearch client"""
        # Create client with headers directly in the constructor
        self.es = Elasticsearch([self.es_host], headers=self.headers)
        logging.info(f"Elasticsearch client initialized for {self.es_host}")

        # Test connection
        try:
            info = self.es.info()
            logging.info(
                f"Connected to Elasticsearch cluster: {info['name']} (version: {info.get('version', {}).get('number', 'unknown')})"
            )
        except Exception as e:
            logging.error(f"Failed to connect to Elasticsearch: {e}")
            raise

    async def close_client(self):
        """Close Elasticsearch client"""
        if self.es:
            self.es.close()
            logging.info("Elasticsearch client closed")

    async def create_indexes(self, delete_existing=False):
        if delete_existing:
            """Create indexes with proper mappings"""

            # Delete existing indexes if they exist
            try:
                if self.es.indices.exists(index=self.property_index):
                    self.es.indices.delete(index=self.property_index)
                    logging.info(f"Deleted existing index: {self.property_index}")
            except:
                pass

            try:
                if self.es.indices.exists(index=self.suggestion_index):
                    self.es.indices.delete(index=self.suggestion_index)
                    logging.info(f"Deleted existing index: {self.suggestion_index}")
            except:
                pass

        else:
            # If indexes exist, log it and continue
            try:
                property_exists = self.es.indices.exists(index=self.property_index)
                suggestion_exists = self.es.indices.exists(index=self.suggestion_index)

                if property_exists and suggestion_exists:
                    logging.info(f"Indexes already exist. Skipping deletion.")
            except Exception as e:
                logging.error(f"Error checking for existing indexes: {e}")

        # Property index mapping
        property_mapping = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "persian": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "persian_normalizer"],
                        },
                        "persian_edge_ngram": {
                            "tokenizer": "edge_ngram_tokenizer",
                            "filter": ["lowercase", "persian_normalizer"],
                        },
                    },
                    "tokenizer": {
                        "edge_ngram_tokenizer": {
                            "type": "edge_ngram",
                            "min_gram": 1,
                            "max_gram": 15,
                            "token_chars": ["letter", "digit"],
                        }
                    },
                    "filter": {"persian_normalizer": {"type": "persian_normalization"}},
                }
            },
            "mappings": {
                "properties": {
                    "external_id": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "persian"},
                    "description": {"type": "text", "analyzer": "persian"},
                    "price": {"type": "long"},
                    "price_per_meter": {"type": "long"},
                    "area": {"type": "long"},
                    "land_area": {"type": "long"},
                    "bedrooms": {"type": "integer"},
                    "property_type_suggest": {
                        "type": "completion",
                        "analyzer": "persian",
                        "contexts": [
                            {"name": "location", "type": "category"},
                            {"name": "stage", "type": "category"},
                        ],
                    },
                    "bedrooms_suggest": {
                        "type": "completion",
                        "analyzer": "persian",
                        "contexts": [{"name": "property_type", "type": "category"}],
                    },
                    "year_built": {"type": "integer"},
                    "has_parking": {"type": "boolean"},
                    "has_storage": {"type": "boolean"},
                    "has_balcony": {"type": "boolean"},
                    "floor_info": {"type": "keyword"},
                    "building_direction": {"type": "keyword"},
                    "renovation_status": {"type": "keyword"},
                    "title_deed_type": {"type": "keyword"},
                    "floor_material": {"type": "keyword"},
                    "bathroom_type": {"type": "keyword"},
                    "cooling_system": {"type": "keyword"},
                    "heating_system": {"type": "keyword"},
                    "hot_water_system": {"type": "keyword"},
                    "attributes": {"type": "nested"},
                    "image_urls": {"type": "keyword"},
                    "location": {
                        "type": "object",
                        "properties": {
                            "neighborhood": {"type": "keyword"},
                            "city": {"type": "keyword"},
                            "district": {"type": "keyword"},
                            "coordinates": {"type": "geo_point"},
                        },
                    },
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                }
            },
        }

        # Suggestion index mapping with search_as_you_type
        suggestion_mapping = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "persian": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "persian_normalizer"],
                        }
                    },
                    "filter": {"persian_normalizer": {"type": "persian_normalization"}},
                }
            },
            "mappings": {
                "properties": {
                    "suggestion_text": {
                        "type": "search_as_you_type",
                        "max_shingle_size": 3,
                        "analyzer": "persian",
                    },
                    "bedrooms.suggest": {
                        "type": "completion",
                        "analyzer": "persian",
                        "contexts": [{"name": "property_type", "type": "category"}],
                    },
                    "suggestion_type": {"type": "keyword"},
                    "context": {"type": "keyword"},
                    "priority": {"type": "integer"},
                    "city": {"type": "keyword"},
                    "district": {"type": "keyword"},
                    "neighborhood": {"type": "keyword"},
                    "min_price": {"type": "long"},
                    "max_price": {"type": "long"},
                    "min_bedrooms": {"type": "integer"},
                    "max_bedrooms": {"type": "integer"},
                    "property_types": {"type": "keyword"},
                    "features": {"type": "keyword"},
                    "created_at": {"type": "date"},
                },
            },
        }

        # Create property index
        try:
            self.es.indices.create(index=self.property_index, body=property_mapping)
            logging.info(f"Created property index: {self.property_index}")
        except Exception as e:
            logging.error(f"Error creating property index: {e}")
            raise

        # Create suggestion index
        try:
            self.es.indices.create(index=self.suggestion_index, body=suggestion_mapping)
            logging.info(f"Created suggestion index: {self.suggestion_index}")
        except Exception as e:
            logging.error(f"Error creating suggestion index: {e}")
            raise

    # Ensure the Elasticsearch index_property method properly handles attributes
    # Update Elasticsearch index_property method in es_indexer.py

    async def index_property(self, property_data: Dict):
        """Index a single property with improved attribute handling"""
        try:
            # Prepare document with direct field mapping
            doc = {
                "external_id": property_data.get("p_external_id"),
                "title": property_data.get("p_title"),
                "description": property_data.get("p_description"),
                "price": property_data.get("p_price"),
                "price_per_meter": property_data.get("p_price_per_meter"),
                "area": property_data.get("p_area"),
                "land_area": property_data.get("p_land_area"),
                "bedrooms": property_data.get("p_bedrooms"),
                "year_built": property_data.get("p_year_built"),
                "property_type": property_data.get("p_property_type"),
                "has_parking": property_data.get("p_has_parking", False),
                "has_storage": property_data.get("p_has_storage", False),
                "has_balcony": property_data.get("p_has_balcony", False),
                "floor_info": property_data.get("p_floor_info"),
                "building_direction": property_data.get("p_building_direction"),
                "renovation_status": property_data.get("p_renovation_status"),
                "title_deed_type": property_data.get("p_title_deed_type"),
                "floor_material": property_data.get("p_floor_material"),
                "bathroom_type": property_data.get("p_bathroom_type"),
                "cooling_system": property_data.get("p_cooling_system"),
                "heating_system": property_data.get("p_heating_system"),
                "hot_water_system": property_data.get("p_hot_water_system"),
                "attributes": property_data.get("p_attributes", []),
                "image_urls": property_data.get("p_image_urls", []),
                "location": self._extract_location(property_data),
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }

            # For fields that might be missing, try to extract from attributes
            attributes = property_data.get("p_attributes", [])

            # Extract bedrooms if missing
            if doc["bedrooms"] is None:
                for attr in attributes:
                    if attr.get("title") == "اتاق":
                        value = attr.get("value")
                        if value:
                            try:
                                from text_utils import \
                                    parse_persian_number  # type: ignore

                                doc["bedrooms"] = parse_persian_number(value)
                            except (ImportError, Exception):
                                cleaned = re.sub(r"[^\d]", "", value)
                                if cleaned:
                                    doc["bedrooms"] = int(cleaned)
                            break

            # Extract bathroom_type if missing
            if not doc["bathroom_type"]:
                for attr in attributes:
                    title = attr.get("title", "")
                    key = attr.get("key")
                    if (
                        "سرویس بهداشتی" in title
                        and key == "WC"
                        and attr.get("available", False)
                    ):
                        doc["bathroom_type"] = title.replace(
                            "سرویس بهداشتی", ""
                        ).strip()
                        break

            # If property_type is missing, classify it on the fly
            if not doc["property_type"] and doc.get("title"):
                from text_utils import classify_property_type

                doc["property_type"] = classify_property_type(
                    doc.get("title", ""), doc.get("description", "")
                )
                if doc["property_type"]:
                    logging.info(
                        f"[{property_data.get('p_external_id')}] Classified property type during indexing: {doc['property_type']}"
                    )

            # Extract heating_system if missing
            if not doc["heating_system"]:
                for attr in attributes:
                    title = attr.get("title", "")
                    key = attr.get("key")
                    if (
                        "گرمایش" in title
                        and key == "SUNNY"
                        and attr.get("available", False)
                    ):
                        doc["heating_system"] = title.replace("گرمایش", "").strip()
                        break

            # For property type suggestions
            if doc.get("property_type"):
                property_type = doc["property_type"]
                doc["property_type_suggest"] = {
                    "input": [property_type],
                    "contexts": {
                        "location": [doc.get("location", {}).get("city", "تهران")],
                        "stage": ["property_type"],
                    },
                }

            # For bedroom suggestions
            if doc.get("bedrooms"):
                doc["bedrooms_suggest"] = {
                    "input": generate_bedroom_variants(doc["bedrooms"]),
                    "contexts": {"property_type": [doc.get("property_type", "")]},
                }

            # Remove None values
            doc = {k: v for k, v in doc.items() if v is not None}

            # Index the document
            self.es.index(
                index=self.property_index,
                id=property_data.get("p_external_id"),
                document=doc,
            )

            # Generate and index suggestions for this property
            await self._generate_suggestions(doc)

            logging.info(
                f"Successfully indexed property: {property_data.get('p_external_id')}"
            )

        except Exception as e:
            logging.error(
                f"Error indexing property {property_data.get('p_external_id')}: {e}"
            )
            raise

    def _extract_location(self, property_data: Dict) -> Dict:
        """Extract and structure location data"""
        location = property_data.get("p_location", {})
        if isinstance(location, str):
            try:
                # Try to parse JSON string
                if location.startswith("{"):
                    location = json.loads(location)
                else:
                    # Parse location string if it's a string
                    parts = location.split(",")
                    if len(parts) >= 2:
                        return {
                            "neighborhood": parts[0].strip(),
                            "district": parts[1].strip() if len(parts) > 1 else None,
                            "city": "تهران",  # Default to Tehran for now
                            "coordinates": None,  # Can be added later if available
                        }
            except:
                pass

        elif isinstance(location, dict):
            return location

        return {"city": "تهران"}

    async def _generate_suggestions(self, property_doc: Dict):
        """Generate and index suggestions for search_as_you_type"""
        suggestions = []

        # Location suggestions
        location = property_doc.get("location", {})
        city = location.get("city", "")
        district = location.get("district", "")
        neighborhood = location.get("neighborhood", "")

        if neighborhood and city:
            suggestions.append(
                {
                    "suggestion_text": f"{neighborhood}, {city}",
                    "suggestion_type": "location",
                    "context": "initial",
                    "priority": 100,
                    "city": city,
                    "district": district,
                    "neighborhood": neighborhood,
                }
            )

        # Property type with location
        property_type = property_doc.get("property_type", "")
        if property_type and city:
            suggestions.append(
                {
                    "suggestion_text": f"{city} {property_type}",
                    "suggestion_type": "property_type",
                    "context": f"{city}",
                    "priority": 90,
                    "city": city,
                    "property_types": [property_type],
                }
            )

        # Bedroom filters
        bedrooms = property_doc.get("bedrooms")
        if bedrooms and city and property_type:
            bedrooms_text = f"{bedrooms}+" if bedrooms < 5 else f"{bedrooms}"
            suggestions.append(
                {
                    "suggestion_text": f"{city} {property_type} با {bedrooms_text} اتاق",
                    "suggestion_type": "bedroom_filter",
                    "context": f"{city} {property_type}",
                    "priority": 80,
                    "city": city,
                    "property_types": [property_type],
                    "min_bedrooms": bedrooms,
                    "max_bedrooms": bedrooms if bedrooms >= 5 else None,
                }
            )

        # Price filters
        price = property_doc.get("price")
        if price and city and property_type:
            price_millions = price / 1000000
            if price_millions < 1:
                price_text = f"زیر {int(price_millions * 1000)} میلیون"
            else:
                price_text = f"زیر {price_millions:.1f} میلیارد"

            suggestions.append(
                {
                    "suggestion_text": f"{city} {property_type} {price_text}",
                    "suggestion_type": "price_filter",
                    "context": f"{city} {property_type}",
                    "priority": 70,
                    "city": city,
                    "property_types": [property_type],
                    "max_price": price,
                }
            )

        # Feature filters
        features = []
        if property_doc.get("has_parking"):
            features.append("پارکینگ")
        if property_doc.get("has_storage"):
            features.append("انباری")
        if property_doc.get("has_balcony"):
            features.append("بالکن")

        if features and city and property_type:
            for feature in features:
                suggestions.append(
                    {
                        "suggestion_text": f"{city} {property_type} با {feature}",
                        "suggestion_type": "feature_filter",
                        "context": f"{city} {property_type}",
                        "priority": 60,
                        "city": city,
                        "property_types": [property_type],
                        "features": [feature],
                    }
                )

        # Index all suggestions
        for suggestion in suggestions:
            suggestion["created_at"] = datetime.now().isoformat()
            try:
                self.es.index(index=self.suggestion_index, document=suggestion)
            except Exception as e:
                logging.error(f"Error indexing suggestion: {e}")

    async def search_properties(
        self, query: str, filters: Optional[Dict] = None
    ) -> List[Dict]:
        """Search properties with filters"""
        search_query = {"query": {"bool": {"must": []}}}

        # Add text search
        if query:
            search_query["query"]["bool"]["must"].append(
                {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "title^2",
                            "description",
                            "location.neighborhood",
                            "location.district",
                            "property_type^1.5",  # Regular property_type field
                            "property_type.ngram",  # ngram field for partial matches
                        ],
                    }
                }
            )

        # Add filters
        if filters:
            if "price_min" in filters or "price_max" in filters:
                price_range = {}
                if "price_min" in filters:
                    price_range["gte"] = filters["price_min"]
                if "price_max" in filters:
                    price_range["lte"] = filters["price_max"]
                search_query["query"]["bool"]["must"].append(
                    {"range": {"price": price_range}}
                )

            if "bedrooms_min" in filters:
                search_query["query"]["bool"]["must"].append(
                    {"range": {"bedrooms": {"gte": filters["bedrooms_min"]}}}
                )

            if "has_parking" in filters:
                search_query["query"]["bool"]["must"].append(
                    {"term": {"has_parking": filters["has_parking"]}}
                )

            if "has_storage" in filters:
                search_query["query"]["bool"]["must"].append(
                    {"term": {"has_storage": filters["has_storage"]}}
                )

            if "has_balcony" in filters:
                search_query["query"]["bool"]["must"].append(
                    {"term": {"has_balcony": filters["has_balcony"]}}
                )

            if "property_type" in filters:
                search_query["query"]["bool"]["must"].append(
                    {"term": {"property_type": filters["property_type"]}}
                )

        # Execute search
        try:
            response = self.es.search(
                index=self.property_index, body=search_query, size=20
            )

            return [hit["_source"] for hit in response["hits"]["hits"]]

        except Exception as e:
            logging.error(f"Error searching properties: {e}")
            return []

    async def get_suggestions(
        self, query: str, context: str = "initial", limit: int = 10
    ) -> List[Dict]:
        """Get suggestions based on query and context"""
        search_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "type": "bool_prefix",
                                "fields": [
                                    "suggestion_text",
                                    "suggestion_text._2gram",
                                    "suggestion_text._3gram",
                                ],
                            }
                        },
                        {"term": {"context": context}},
                    ]
                }
            },
            "sort": [{"priority": "desc"}],
            "size": limit,
        }

        try:
            response = self.es.search(index=self.suggestion_index, body=search_query)

            return [hit["_source"] for hit in response["hits"]["hits"]]

        except Exception as e:
            logging.error(f"Error getting suggestions: {e}")
            return []
