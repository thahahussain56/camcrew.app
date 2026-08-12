import os
import re

def fix_html_links(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.html'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                
                # 1. Fix standard .html links
                # Find all href="somelink.html" where it's just a word
                # Example: href="sales.html" -> href="/sales"
                # Example: href="index.html" -> href="/"
                
                def link_replacer(match):
                    link = match.group(1)
                    if link == 'index.html':
                        return 'href="/"'
                    elif link.endswith('.html') and '/' not in link:
                        return f'href="/{link[:-5]}"'
                    return match.group(0)
                    
                content = re.sub(r'href=["\']([^"\']+\.html)["\']', link_replacer, content)
                
                # 2. Fix javascript:history.back()
                content = content.replace('href="javascript:history.back()"', 'href="/"')
                
                # 3. Fix customer-profile images
                content = content.replace('src="img\\camcrew_studio_logo_cop_y.png"', 'src="/static/img/camcrew_studio_logo_cop_y.png"')
                content = content.replace('src="img\\camcrew_studio_logo_cop.png"', 'src="/static/img/camcrew_studio_logo_cop.png"')
                
                # 4. Remove extra closing div tags if this file is one of the 3 problematic ones
                if file in ['adminlogin.html', 'header.html', 'sales.html']:
                    # We will parse to find the unbalanced </div>
                    # Simple stack-based approach
                    # Find all indices of <div and </div>
                    div_opens = [m.span() for m in re.finditer(r'<div\b', content, re.IGNORECASE)]
                    div_closes = [m.span() for m in re.finditer(r'</div>', content, re.IGNORECASE)]
                    
                    if len(div_closes) > len(div_opens):
                        # Find which one to remove by doing a forward pass
                        stack = []
                        tokens = []
                        for op in div_opens:
                            tokens.append((op[0], op[1], 'open'))
                        for cl in div_closes:
                            tokens.append((cl[0], cl[1], 'close'))
                            
                        tokens.sort(key=lambda x: x[0])
                        
                        unbalanced_close_idx = None
                        current_depth = 0
                        for t in tokens:
                            if t[2] == 'open':
                                current_depth += 1
                            else:
                                if current_depth == 0:
                                    unbalanced_close_idx = t
                                    break
                                else:
                                    current_depth -= 1
                                    
                        # If we couldn't find one that drops below 0, it means it's at the end
                        if unbalanced_close_idx is None:
                            # Just remove the very last one
                            unbalanced_close_idx = tokens[-1]
                            
                        if unbalanced_close_idx:
                            # Remove it
                            content = content[:unbalanced_close_idx[0]] + content[unbalanced_close_idx[1]:]

                if content != original_content:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"Fixed {file_path}")

if __name__ == "__main__":
    fix_html_links("templates")
