import os
import re

def check_html_errors(directory):
    errors = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.html') or file.endswith('.js'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    
                    # Look for localhost or 127.0.0.1 which might break in production
                    if 'http://localhost' in content or 'http://127.0.0.1' in content:
                        errors.append(f"{file_path}: Hardcoded localhost URL found.")
                        
                    # Look for broken jinja variables
                    if '{{ ' in content and ' }}' not in content:
                        errors.append(f"{file_path}: Potential broken jinja variable.")
                        
                    # Look for unmatched <script> tags
                    script_opens = len(re.findall(r'<script\b', content, re.IGNORECASE))
                    script_closes = len(re.findall(r'</script>', content, re.IGNORECASE))
                    if script_opens != script_closes:
                        errors.append(f"{file_path}: Unmatched script tags (opens: {script_opens}, closes: {script_closes}).")
                        
                    # Unmatched div tags
                    div_opens = len(re.findall(r'<div\b', content, re.IGNORECASE))
                    div_closes = len(re.findall(r'</div>', content, re.IGNORECASE))
                    if div_opens != div_closes:
                        errors.append(f"{file_path}: Unmatched div tags (opens: {div_opens}, closes: {div_closes}).")
                        
    return errors

if __name__ == "__main__":
    print("Checking for HTML/JS logical errors...")
    errs = check_html_errors("templates")
    errs += check_html_errors("static")
    
    if errs:
        for e in errs:
            print(f" - {e}")
    else:
        print("No obvious HTML/JS structural errors found.")
