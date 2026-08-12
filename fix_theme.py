import os
import re

css_addition = """
/* Remove strokes for containers in light theme */
[data-theme="light"] .glass-panel,
[data-theme="light"] .glass-panel:hover,
[data-theme="light"] .service-card,
[data-theme="light"] .service-card:hover,
[data-theme="light"] .product-card,
[data-theme="light"] .product-card:hover,
[data-theme="light"] .rental-card,
[data-theme="light"] .rental-card:hover,
[data-theme="light"] .dashboard-card,
[data-theme="light"] .dashboard-card:hover,
[data-theme="light"] .neon-border-glow,
[data-theme="light"] .neon-border-glow:hover,
html.light .glass-panel,
html.light .glass-panel:hover,
html.light .service-card,
html.light .service-card:hover,
html.light .product-card,
html.light .product-card:hover,
html.light .rental-card,
html.light .rental-card:hover,
html.light .dashboard-card,
html.light .dashboard-card:hover,
html.light .neon-border-glow,
html.light .neon-border-glow:hover {
    border-color: transparent !important;
    border: none !important;
    box-shadow: none !important;
}
"""

with open('static/css/styles.css', 'a', encoding='utf-8') as f:
    f.write("\n" + css_addition)

print("CSS appended to styles.css")

# Now, fix the inline styles in templates
def remove_hardcoded_backgrounds(directory):
    patterns = [
        # Remove background:#... or background-color:#... 
        # but don't strip the whole style attribute if there are other styles
        (re.compile(r'(?<=style=[\'\"]).*?(background(-color)?\s*:\s*(#141414|#1a1a1c|#14181f|#0e0e10|#1e293b|#000|#0A0A0B|#000000)[;]?\s*)', re.IGNORECASE), ''),
    ]
    for root, dirs, files in os.walk(directory):
        for f in files:
            if f.endswith('.html'):
                path = os.path.join(root, f)
                with open(path, 'r', encoding='utf-8') as file:
                    content = file.read()
                
                new_content = content
                for p, rep in patterns:
                    new_content = p.sub(rep, new_content)
                
                # Cleanup empty style attributes
                new_content = re.sub(r'style=["\']\s*["\']', '', new_content)
                
                if new_content != content:
                    with open(path, 'w', encoding='utf-8') as file:
                        file.write(new_content)
                    print(f"Fixed inline backgrounds in {path}")

remove_hardcoded_backgrounds('templates')
