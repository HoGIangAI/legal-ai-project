import os
import sys
from flask import Flask, render_template, request, jsonify, send_file
import json
from pathlib import Path
import fnmatch
from datetime import datetime

# Thêm thư mục hiện tại vào path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = str(current_dir / 'uploads')
app.config['OUTPUT_FOLDER'] = str(current_dir / 'outputs')

# Đảm bảo thư mục tồn tại
Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True)
Path(app.config['OUTPUT_FOLDER']).mkdir(exist_ok=True)

class CodeAnalyzer:
    def __init__(self):
        self.code_extensions = {
            '.py', '.js', '.jsx', '.ts', '.tsx', '.vue', '.java',
            '.html', '.css', '.scss', '.json', '.yaml', '.yml',
            '.md', '.txt', '.env', '.config', '.sql', '.sh'
        }
        # === THAY ĐỔI Ở ĐÂY ===
        # Thêm 'legal-ai-venv' vào danh sách các thư mục cần bỏ qua
        self.ignore_dirs = {'.git', '__pycache__', 'node_modules', 'venv', 'legal-ai-venv'}

    def scan_project(self, project_path: str):
        context = {}
        
        # Xử lý đường dẫn tương đối/ tuyệt đối
        if project_path == '.':
            project_path_obj = Path.cwd()
        else:
            project_path_obj = Path(project_path)
            if not project_path_obj.is_absolute():
                project_path_obj = Path.cwd() / project_path_obj
        
        project_path_obj = project_path_obj.resolve()
        
        print(f"🔍 Scanning: {project_path_obj}")
        print(f"📁 Exists: {project_path_obj.exists()}")
        print(f"📁 Is directory: {project_path_obj.is_dir()}")
        
        if not project_path_obj.exists():
            return {'success': False, 'error': f'Path does not exist: {project_path_obj}'}
        
        if not project_path_obj.is_dir():
            return {'success': False, 'error': f'Path is not a directory: {project_path_obj}'}
        
        try:
            file_count = 0
            for root, dirs, files in os.walk(project_path_obj):
                # Remove ignored directories
                dirs[:] = [d for d in dirs if d not in self.ignore_dirs]
                
                for file in files:
                    file_path = Path(root) / file
                    if file_path.suffix.lower() in self.code_extensions:
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            
                            rel_path = str(file_path.relative_to(project_path_obj))
                            context[rel_path] = {
                                'path': rel_path,
                                'content': content,
                                'lines': content.count('\n') + 1,
                                'size_kb': round(len(content) / 1024, 2),
                                'language': self.get_language(file_path.suffix),
                                'extension': file_path.suffix.lower()
                            }
                            file_count += 1
                            print(f"✅ Scanned: {rel_path}")
                        except Exception as e:
                            print(f"⚠️  Could not read {file_path}: {e}")
                            continue
            
            print(f"📊 Scan complete: {file_count} files found")
            return {'success': True, 'context': context, 'total_files': len(context)}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_language(self, extension: str):
        lang_map = {
            '.py': 'Python', '.js': 'JavaScript', '.jsx': 'React JSX',
            '.ts': 'TypeScript', '.tsx': 'React TSX', '.vue': 'Vue',
            '.java': 'Java', '.html': 'HTML', '.css': 'CSS',
            '.scss': 'SCSS', '.json': 'JSON', '.yaml': 'YAML',
            '.md': 'Markdown', '.sql': 'SQL', '.sh': 'Shell'
        }
        return lang_map.get(extension, 'Unknown')

    def extract_selected_files(self, context, patterns):
        selected_files = {}
        
        for pattern in patterns:
            pattern = pattern.strip()
            for file_path, file_info in context.items():
                file_path_clean = file_path.strip()
                
                if pattern == file_path_clean:
                    selected_files[file_path] = file_info
                    continue
                    
                if '*' in pattern:
                    if fnmatch.fnmatch(file_path_clean, pattern):
                        selected_files[file_path] = file_info
                        continue
                
                if pattern.lower() in file_path_clean.lower():
                    selected_files[file_path] = file_info
                    continue
        
        return selected_files

analyzer = CodeAnalyzer()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return '', 204

@app.route('/scan', methods=['POST'])
def scan_project():
    try:
        data = request.get_json()
        project_path = data.get('project_path', '.')
        
        print(f"📁 Scan request for: {project_path}")
        result = analyzer.scan_project(project_path)
        
        if result['success']:
            context_file = Path(app.config['OUTPUT_FOLDER']) / 'context.json'
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(result['context'], f, indent=2, ensure_ascii=False)
            print(f"💾 Context saved: {context_file}")
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/files', methods=['GET'])
def get_files_list():
    try:
        context_file = Path(app.config['OUTPUT_FOLDER']) / 'context.json'
        if not context_file.exists():
            return jsonify({'success': False, 'error': 'No context found. Please scan a project first.'})
        
        with open(context_file, 'r', encoding='utf-8') as f:
            context = json.load(f)
        
        files_by_type = {}
        for file_path, info in context.items():
            ext = info.get('extension', '')
            if ext not in files_by_type:
                files_by_type[ext] = []
            files_by_type[ext].append({
                'path': file_path,
                'lines': info.get('lines', 0),
                'size_kb': info.get('size_kb', 0),
                'language': info.get('language', 'Unknown')
            })
        
        return jsonify({
            'success': True,
            'total_files': len(context),
            'files_by_type': files_by_type
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/extract', methods=['POST'])
def extract_files():
    try:
        data = request.get_json()
        selected_patterns = [p.strip() for p in data.get('patterns', []) if p.strip()]
        
        print(f"🎯 Extract request with patterns: {selected_patterns}")
        
        if not selected_patterns:
            return jsonify({'success': False, 'error': 'No valid patterns provided'})
        
        context_file = Path(app.config['OUTPUT_FOLDER']) / 'context.json'
        if not context_file.exists():
            return jsonify({'success': False, 'error': 'No context found. Please scan a project first.'})
        
        with open(context_file, 'r', encoding='utf-8') as f:
            context = json.load(f)
        
        selected_files = analyzer.extract_selected_files(context, selected_patterns)
        
        print(f"📄 Found {len(selected_files)} matching files")
        
        if not selected_files:
            return jsonify({'success': False, 'error': 'No files matched the patterns'})
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = Path(app.config['OUTPUT_FOLDER']) / f'selected_code_{timestamp}.json'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(selected_files, f, indent=2, ensure_ascii=False)
        
        text_file = Path(app.config['OUTPUT_FOLDER']) / f'selected_code_{timestamp}.txt'
        with open(text_file, 'w', encoding='utf-8') as f:
            for file_path, info in selected_files.items():
                f.write("=" * 60 + "\n")
                f.write(f"FILE: {file_path}\n")
                f.write(f"LINES: {info['lines']} | SIZE: {info['size_kb']}KB\n")
                f.write("=" * 60 + "\n")
                f.write(info['content'])
                f.write("\n\n")
        
        total_lines = sum(info['lines'] for info in selected_files.values())
        total_size = sum(info['size_kb'] for info in selected_files.values())
        
        return jsonify({
            'success': True,
            'selected_files': len(selected_files),
            'total_lines': total_lines,
            'total_size_kb': round(total_size, 2),
            'output_files': {
                'json': str(output_file),
                'text': str(text_file)
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/download/<filename>')
def download_file(filename):
    try:
        file_path = Path(app.config['OUTPUT_FOLDER']) / filename
        if file_path.exists():
            return send_file(file_path, as_attachment=True)
        else:
            return jsonify({'success': False, 'error': 'File not found'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    print("🚀 Starting Code Analyzer Web UI...")
    print("📁 Location: /home/asus/legal_ai_infrastructure/legal-ai-implementation/legal-ai-platform/my_code_analyzer")
    print("🌐 Visit: http://127.0.0.1:5000")
    print("🔧 Debug mode: ON")
    app.run(debug=True, host='0.0.0.0', port=5000)

