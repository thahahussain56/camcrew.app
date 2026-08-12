import os
import re

def check_static_files(directory):
    missing_files = []
    
    # Regex to find href="...", src="..."
    pattern = re.compile(r'(?:href|src)=["\']([^"\']+)["\']', re.IGNORECASE)
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.html') or file.endswith('.css') or file.endswith('.js'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    matches = pattern.findall(content)
                    
                    for match in matches:
                        # Ignore external links or jinja templates logic
                        if match.startswith('http') or match.startswith('//') or match.startswith('data:'):
                            continue
                        if '{' in match or '}' in match:
                            continue
                            
                        # Strip query parameters (e.g. ?v=1)
                        clean_match = match.split('?')[0].split('#')[0]
                        
                        # Handle absolute paths pointing to /static/
                        if clean_match.startswith('/static/'):
                            # Map /static/ to the local static directory
                            local_path = os.path.join('static', clean_match[8:])
                            if not os.path.exists(local_path):
                                missing_files.append((file_path, clean_match))
                        elif not clean_match.startswith('/') and not clean_match.startswith('#'):
                            # Handle relative paths
                            local_path = os.path.join(root, clean_match)
                            if not os.path.exists(local_path):
                                missing_files.append((file_path, clean_match))
                                
    return missing_files

if __name__ == "__main__":
    print("Checking for missing static assets...")
    errs = check_static_files("templates")
    errs += check_static_files("static")
    
    if errs:
        # Deduplicate
        errs = list(set(errs))
        print("Missing static files found:")
        for file_path, missing in sorted(errs):
            print(f" - {file_path} references missing file: {missing}")
    else:
        print("No missing static files found.")
