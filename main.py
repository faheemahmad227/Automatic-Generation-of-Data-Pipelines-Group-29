"""
Main Flask Application - Web interface for the Automatic Data Pipeline Generator.
"""
import os
import json
import zipfile
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from werkzeug.utils import secure_filename
import structlog

# Import our modules
from app.config import (
    UPLOAD_FOLDER, GENERATED_FOLDER, ALLOWED_EXTENSIONS,
    MAX_CONTENT_LENGTH, TRANSFORMATION_TYPES, V_MODEL_PHASES
)
from app.source_analyzer import SourceAnalyzer
from app.ai_analyzer import AIAnalyzer
from app.pipeline_generator import PipelineGenerator

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logger = structlog.get_logger()

# Initialize Flask app
app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')

app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH
app.config['UPLOAD_FOLDER'] = str(UPLOAD_FOLDER)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

# Initialize analyzers and generators
source_analyzer = SourceAnalyzer()
ai_analyzer = AIAnalyzer()
pipeline_generator = PipelineGenerator()

# TODO: Replace this with Redis or something when we go to prod
# Store for pipeline execution metrics (in-memory for demo)
pipeline_metrics = []
execution_history = []


def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    """Render the main dashboard."""
    return render_template('index.html',
                         transformation_types=TRANSFORMATION_TYPES,
                         v_model_phases=V_MODEL_PHASES,
                         recent_pipelines=get_recent_pipelines())


@app.route('/generate', methods=['GET'])
def generate_page():
    """Render the pipeline generation page."""
    return render_template('generate.html',
                         transformation_types=TRANSFORMATION_TYPES,
                         v_model_phases=V_MODEL_PHASES)


@app.route('/analyze', methods=['GET'])
def analyze_page():
    """Render the source analysis page."""
    return render_template('analyze.html')


@app.route('/monitor', methods=['GET'])
def monitor_page():
    """Render the monitoring dashboard."""
    return render_template('monitor.html',
                         metrics=pipeline_metrics,
                         history=execution_history)


@app.route('/api/analyze', methods=['POST'])
def api_analyze_source():
    """
    API endpoint to analyze uploaded data source.
    
    Expects: File upload with key 'file'
    Returns: JSON with schema and quality metrics
    """
    logger.info("Analyze request received")
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': f'File type not supported. Allowed: {", ".join(ALLOWED_EXTENSIONS)}'}), 400
    
    try:
        # Save uploaded file
        filename = secure_filename(file.filename)
        filepath = Path(UPLOAD_FOLDER) / filename
        file.save(filepath)
        
        logger.info("File uploaded", filename=filename)
        
        # Analyze the file
        schema, quality = source_analyzer.analyze(str(filepath))
        suggestions = source_analyzer.suggest_transformations(schema, quality)
        preview = source_analyzer.get_preview(rows=10)
        
        # Get AI recommendations if available
        recommendations = ai_analyzer.get_recommendations(schema, quality)
        
        return jsonify({
            'success': True,
            'filename': filename,
            'schema': schema,
            'quality': quality,
            'suggestions': suggestions,
            'preview': preview,
            'recommendations': recommendations
        })
        
    except Exception as e:
        logger.error("Analysis failed", error=str(e))
        return jsonify({'error': str(e)}), 500


@app.route('/api/generate', methods=['POST'])
def api_generate_pipeline():
    """
    API endpoint to generate pipeline from requirements.
    
    Expects: JSON with 'description' and optional 'specifications'
    Returns: JSON with generated file paths
    """
    logger.info("Generate request received")
    
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    description = data.get('description', '')
    specifications = data.get('specifications', {})
    use_ai = data.get('use_ai', True)
    
    if not description and not specifications:
        return jsonify({'error': 'Please provide a description or specifications'}), 400
    
    try:
        # Analyze requirements with AI if description provided
        if description and use_ai:
            specs = ai_analyzer.analyze_requirements(description, specifications.get('schema'))
            # Merge with any provided specifications
            specs.update({k: v for k, v in specifications.items() if v})
        else:
            specs = specifications
            if not specs.get('pipeline_name'):
                specs['pipeline_name'] = 'custom_pipeline'
        
        # Generate pipeline code with AI
        ai_code = None
        if use_ai:
            ai_code = ai_analyzer.generate_pipeline_code(specs)
        
        # Generate complete pipeline package
        generated = pipeline_generator.generate_pipeline(specs, ai_code)
        
        # Record in history
        execution_history.append({
            'timestamp': datetime.now().isoformat(),
            'action': 'generate',
            'pipeline_name': specs.get('pipeline_name'),
            'status': 'success'
        })
        
        return jsonify({
            'success': True,
            'specifications': specs,
            'generated_files': generated,
            'message': f"Pipeline '{specs.get('pipeline_name')}' generated successfully"
        })
        
    except Exception as e:
        logger.error("Generation failed", error=str(e))
        return jsonify({'error': str(e)}), 500


@app.route('/api/generate-from-file', methods=['POST'])
def api_generate_from_file():
    """
    Generate pipeline from uploaded requirements file (YAML/JSON).
    """
    logger.info("Generate from file request received")
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    try:
        content = file.read().decode('utf-8')
        
        # probably should validate the file format here but works for now
        if file.filename.endswith('.yaml') or file.filename.endswith('.yml'):
            import yaml
            specifications = yaml.safe_load(content)
        else:
            specifications = json.loads(content)
        
        # Generate pipeline
        ai_code = ai_analyzer.generate_pipeline_code(specifications)
        generated = pipeline_generator.generate_pipeline(specifications, ai_code)
        
        return jsonify({
            'success': True,
            'specifications': specifications,
            'generated_files': generated
        })
        
    except Exception as e:
        logger.error("Generation from file failed", error=str(e))
        return jsonify({'error': str(e)}), 500


@app.route('/api/validate', methods=['POST'])
def api_validate_config():
    """
    Validate pipeline configuration against JSON schema.
    """
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No configuration provided'}), 400
    
    errors = []
    warnings = []
    
    # Basic validation
    if not data.get('pipeline_name'):
        errors.append("Pipeline name is required")
    
    if not data.get('source'):
        errors.append("Source configuration is required")
    
    if not data.get('destination'):
        errors.append("Destination configuration is required")
    
    # Validate transformations
    transformations = data.get('transformations', [])
    for i, t in enumerate(transformations):
        if not t.get('type'):
            errors.append(f"Transformation {i+1}: type is required")
        elif t.get('type') not in TRANSFORMATION_TYPES:
            warnings.append(f"Transformation {i+1}: '{t.get('type')}' is not a standard type")
    
    return jsonify({
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    })


@app.route('/api/download/<path:filename>')
def api_download_file(filename):
    """Download a generated file."""
    try:
        return send_from_directory(str(GENERATED_FOLDER), filename, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 404


@app.route('/api/download-zip/<pipeline_dir>')
def api_download_zip(pipeline_dir):
    """Download entire pipeline as ZIP."""
    try:
        dir_path = GENERATED_FOLDER / pipeline_dir
        if not dir_path.exists():
            return jsonify({'error': 'Pipeline not found'}), 404
        
        # Create ZIP file
        zip_path = GENERATED_FOLDER / f"{pipeline_dir}.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in dir_path.rglob('*'):
                if file.is_file():
                    zipf.write(file, file.relative_to(dir_path))
        
        return send_file(zip_path, as_attachment=True, download_name=f"{pipeline_dir}.zip")
        
    except Exception as e:
        logger.error("ZIP download failed", error=str(e))
        return jsonify({'error': str(e)}), 500


@app.route('/api/pipelines')
def api_list_pipelines():
    """List all generated pipelines."""
    pipelines = []
    
    for item in GENERATED_FOLDER.iterdir():
        if item.is_dir():
            spec_file = item / 'specifications.json'
            if spec_file.exists():
                with open(spec_file) as f:
                    specs = json.load(f)
                pipelines.append({
                    'name': specs.get('pipeline_name'),
                    'description': specs.get('description'),
                    'directory': item.name,
                    'created': datetime.fromtimestamp(item.stat().st_mtime).isoformat()
                })
    
    return jsonify({'pipelines': sorted(pipelines, key=lambda x: x['created'], reverse=True)})


@app.route('/api/metrics')
def api_get_metrics():
    """Get pipeline execution metrics for monitoring."""
    return jsonify({
        'metrics': pipeline_metrics,
        'history': execution_history[-50:],  # Last 50 entries
        'summary': {
            'total_executions': len(execution_history),
            'successful': sum(1 for h in execution_history if h.get('status') == 'success'),
            'failed': sum(1 for h in execution_history if h.get('status') == 'failed')
        }
    })


@app.route('/api/execute', methods=['POST'])
def api_execute_pipeline():
    """
    Execute a generated pipeline (dry-run by default for safety).
    """
    data = request.get_json()
    
    pipeline_dir = data.get('pipeline_directory')
    source_path = data.get('source_path')
    destination_path = data.get('destination_path')
    dry_run = data.get('dry_run', True)
    
    if not all([pipeline_dir, source_path, destination_path]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    # For safety in demo, we just simulate execution
    # I'll implement actual execution once we have proper env isolation
    result = {
        'status': 'dry_run' if dry_run else 'simulated',
        'pipeline': pipeline_dir,
        'source': source_path,
        'destination': destination_path,
        'timestamp': datetime.now().isoformat(),
        'message': 'Execution simulated for demo. Deploy pipeline to run actual execution.'
    }
    
    execution_history.append({
        'timestamp': result['timestamp'],
        'action': 'execute',
        'pipeline_name': pipeline_dir,
        'status': result['status']
    })
    
    return jsonify(result)


def get_recent_pipelines(limit=5):
    """Get recently generated pipelines for dashboard."""
    pipelines = []
    
    if GENERATED_FOLDER.exists():
        for item in sorted(GENERATED_FOLDER.iterdir(), 
                          key=lambda x: x.stat().st_mtime, 
                          reverse=True)[:limit]:
            if item.is_dir():
                spec_file = item / 'specifications.json'
                if spec_file.exists():
                    with open(spec_file) as f:
                        specs = json.load(f)
                    pipelines.append({
                        'name': specs.get('pipeline_name', item.name),
                        'description': specs.get('description', ''),
                        'directory': item.name,
                        'created': datetime.fromtimestamp(item.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
                    })
    
    return pipelines


# Error handlers
@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'File too large. Maximum size is 16MB'}), 413


@app.errorhandler(500)
def server_error(e):
    logger.error("Server error", error=str(e))
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Ensure directories exist
    UPLOAD_FOLDER.mkdir(exist_ok=True)
    GENERATED_FOLDER.mkdir(exist_ok=True)
    
    logger.info("Starting Data Pipeline Generator", 
               upload_folder=str(UPLOAD_FOLDER),
               generated_folder=str(GENERATED_FOLDER))
    
    # Debug mode should be False in production obviously
    app.run(host='0.0.0.0', port=5000, debug=True)