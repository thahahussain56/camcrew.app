import os
from jinja2 import Environment, FileSystemLoader

def validate_templates(directory):
    env = Environment(loader=FileSystemLoader(directory))
    errors = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.html'):
                rel_path = os.path.relpath(os.path.join(root, file), directory)
                # Replace Windows slashes with forward slashes for Jinja
                rel_path = rel_path.replace("\\", "/")
                try:
                    env.get_template(rel_path)
                except Exception as e:
                    errors.append(f"{rel_path}: {str(e)}")
                    
    return errors

if __name__ == "__main__":
    template_dir = "templates"
    if os.path.exists(template_dir):
        print("Checking templates for syntax errors...")
        errs = validate_templates(template_dir)
        if errs:
            print("Errors found in templates:")
            for e in errs:
                print(f" - {e}")
        else:
            print("No Jinja syntax errors found in templates.")
    else:
        print("Templates directory not found.")
