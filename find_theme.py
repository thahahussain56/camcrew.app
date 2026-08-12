import os
import re

issues = []

# Look for hardcoded styles like background: #0a0a0a, background-color: #111, color: #fff
patterns = [
    re.compile(r'style=[\'\"].*?background[^:]*:\s*(#0[0-9a-fA-F]{2}|#1[0-9a-fA-F]{2}|#0[0-9a-fA-F]{5}|#1[0-9a-fA-F]{5}).*?[\'\"]', re.IGNORECASE),
    re.compile(r'style=[\'\"].*?background[^:]*:\s*rgba?\(\s*[0-3]\d?\s*,\s*[0-3]\d?\s*,\s*[0-3]\d?\s*,.*?[\'\"]', re.IGNORECASE),
]

for root, dirs, files in os.walk('templates'):
    for f in files:
        if f.endswith('.html'):
            path = os.path.join(root, f)
            with open(path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                for i, line in enumerate(lines):
                    for p in patterns:
                        if p.search(line):
                            # Skip if it is a modal overlay backdrop
                            if 'backdrop-filter' in line or 'rgba(14,23,42,0.45)' in line:
                                continue
                            # Replace non-ascii chars to avoid encode error
                            clean_line = line.strip().encode('ascii', 'ignore').decode('ascii')
                            issues.append(f'{f}:{i+1} -> {clean_line[:100]}...')

for i in set(issues):
    print(i)
