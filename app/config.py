"""
Configuration settings for the Data Pipeline Generator.
"""
import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Upload and output directories
UPLOAD_FOLDER = BASE_DIR / "uploads"
GENERATED_FOLDER = BASE_DIR / "generated_pipelines"
TEMPLATES_FOLDER = BASE_DIR / "templates_jinja"

# Ensure directories exist
UPLOAD_FOLDER.mkdir(exist_ok=True)
GENERATED_FOLDER.mkdir(exist_ok=True)
TEMPLATES_FOLDER.mkdir(exist_ok=True)

# AI Gateway Configuration (Uni Paderborn)
AI_GATEWAY_URL = os.getenv("AI_GATEWAY_URL", "https://ai-gateway.uni-paderborn.de/v1/")
AI_GATEWAY_API_KEY = os.getenv("AI_GATEWAY_API_KEY", "sk-JajdqtLcvjAF7QPVavcRAw")

# Alternative: OpenAI-compatible API
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

# Application settings
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16 MB max file size
ALLOWED_EXTENSIONS = {'csv', 'json', 'xlsx', 'xls', 'parquet'}

# Pipeline execution settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Monitoring settings
METRICS_RETENTION_DAYS = 30

# Supported transformation types
TRANSFORMATION_TYPES = [
    'normalize',
    'aggregate', 
    'filter',
    'map',
    'validate',
    'deduplicate',
    'enrich',
    'custom'
]

# V-Model phases
V_MODEL_PHASES = [
    'Requirements',
    'System Design',
    'Architecture Design',
    'Module Design',
    'Implementation',
    'Unit Testing',
    'Integration Testing',
    'System Testing',
    'Acceptance Testing'
]
