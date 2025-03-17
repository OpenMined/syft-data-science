from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter
from IPython.display import display, HTML
import uuid
css = """
<style>
    .review-container-id {
        font-family: Arial, sans-serif;
        padding: 20px;
    }
    .section-id {
        margin-bottom: 20px;
        padding: 15px;
        border: 1px solid #ddd;
        border-radius: 5px;
    }
    .decision-id { 
    background: #f8f9fa; 
    }
    .summary-id { 
    background: #e9f5ff; 
    }
    .code-id { 
    background: #f4f4f4; font-family: monospace; white-space: pre-wrap; 
    }
    .section-title-id {
        font-weight: bold;
        margin-bottom: 5px;
    }
</style>"""

content_template = """
<div class="review-container-{id}">
    <div class="section decision-{id}">
        <div class="section-title-{id}">Decision</div>
        <div>{decision}</div>
    </div>

    <div class="section-{id} summary-{id}">
        <div class="section-title-{id}">Summary</div>
        <div>{explanation}</div>
    </div>

    <div class="section-{id} code-{id}">
        <div class="section-title-{id}">Code</div>
        {code_html}
    </div>
</div>
"""

formatter = HtmlFormatter(style="default", linenos="inline")
# formatter.nobackground = False
# css = formatter.get_style_defs('.highlight')
display(HTML(f'<style>{formatter.get_style_defs(".highlight")}</style>'))

# Only works for python
def generate_html_comments(code_comments):
    code_comments_html = {}
    for line in code_comments.split('\n'):
        print(line)
        if len(line.split('#')) > 1:
            comment = '#' + line.split('#')[1]
            line_no = int(line.split(' ')[1][:-1])
            if comment.split(":")[0] == "# DANGER":
                style = 'style="color: #DC0001; font-style:italic;"'
            elif comment.split(":")[0] == "# INFO":
                style = 'style="color: #cca300; font-style:italic;"'
            else:
                style = 'class="c1"'
            code_comments_html[line_no]=f'</span>    <span {style}>{comment}</span>'
    return code_comments_html

def generate_html(code, decision, explanation, code_comments):
    
    code_comments_html = generate_html_comments(code_comments)
    formatter = HtmlFormatter(style="default", linenos="inline")
    
    new_html_lines = []
    # Only works for python
    for i, line in enumerate(highlight(code, PythonLexer(), formatter).split('\n')):
        if i + 1 in code_comments_html:
            line += code_comments_html[i+1]
        new_html_lines.append(line)
        
    code_html = '\n'.join(new_html_lines)
    uid = str(uuid.uuid4())
    formatter_css = f'<style>{formatter.get_style_defs(".highlight")}</style>'
    html_content = content_template.format(id=uid, decision=decision, explanation=explanation, code_html=code_html)
    full_html = formatter_css + css.replace('id', uid) + html_content
    
    return full_html