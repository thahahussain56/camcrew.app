import os
import re

issues = []
pattern = re.compile(r'style=[\'\"].*?(color|background)[^:]*:\s*(#|rgb|rgba|black|white|dark|light).*?[\'\"]', re.IGNORECASE)

for root, dirs, files in os.walk('templates'):
    for f in files:
        if f.endswith('.html'):
            path = os.path.join(root, f)
            with open(path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                for i, line in enumerate(lines):
                    if pattern.search(line):
                        issues.append(f"{path}:{i+1} -> {line.strip()}")

for i in issues:
    print(i)
