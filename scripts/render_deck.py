"""Render deck/RFI_Copilot_Business_Case.md (slides separated by ---) to 16:9 HTML, then PDF via headless Chrome."""
import html, os, re, subprocess, sys

ROOT = os.path.join(os.path.dirname(__file__), "..", "deck")
SRC = os.path.join(ROOT, "RFI_Copilot_Business_Case.md")
HTML_OUT = os.path.join(ROOT, "RFI_Copilot_Business_Case.html")
PDF_OUT = os.path.join(ROOT, "RFI_Copilot_Business_Case.pdf")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

def inline(t):
    t = html.escape(t)
    t = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", t)
    t = re.sub(r"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])", r"<i>\1</i>", t)
    return re.sub(r"`(.+?)`", r"<code>\1</code>", t)

def block(lines):
    out, i = [], 0
    while i < len(lines):
        l = lines[i]
        if not l.strip(): i += 1; continue
        if l.startswith("# "): out.append(f"<h1>{inline(l[2:])}</h1>"); i += 1; continue
        if l.startswith("## "): out.append(f"<h2>{inline(l[3:])}</h2>"); i += 1; continue
        if l.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r":?-{3,}:?", c) for c in cells): rows.append(cells)
                i += 1
            head, body = rows[0], rows[1:]
            out.append("<table><tr>" + "".join(f"<th>{inline(c)}</th>" for c in head) + "</tr>" +
                       "".join("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>" for r in body) + "</table>")
            continue
        if re.match(r"^(- |\d+\. )", l):
            tag = "ol" if re.match(r"^\d+\. ", l) else "ul"; items = []
            while i < len(lines) and re.match(r"^(- |\d+\. )", lines[i]):
                items.append(re.sub(r"^(- |\d+\. )", "", lines[i])); i += 1
            out.append(f"<{tag}>" + "".join(f"<li>{inline(x)}</li>" for x in items) + f"</{tag}>"); continue
        if l.startswith("> "): out.append(f"<blockquote>{inline(l[2:])}</blockquote>"); i += 1; continue
        out.append(f"<p>{inline(l)}</p>"); i += 1
    return "\n".join(out)

CSS = """
@page { size: 13.333in 7.5in; margin: 0 }
* { box-sizing: border-box }
body { margin: 0; font-family: 'Helvetica Neue', Arial, sans-serif; color: #14202B }
.slide { width: 13.333in; height: 7.5in; padding: 0.55in 0.75in 0.5in; page-break-after: always; position: relative; overflow: hidden;
         border-top: 0.12in solid #C21E2E; background: #fff }
.slide.title { background: #14202B; color: #fff; display: flex; flex-direction: column; justify-content: center }
.slide.title h1 { font-size: 46pt; margin: 0 0 18px; letter-spacing: -0.02em } .slide.title p { font-size: 17pt; color: #C6D2DA; max-width: 10.5in }
.slide.title b { color: #fff }
h1 { font-size: 30pt; margin: 0 0 14px } h2 { font-size: 25pt; margin: 0 0 16px; letter-spacing: -0.01em }
p, li { font-size: 13.5pt; line-height: 1.42 } li { margin-bottom: 6px } ul, ol { margin: 6px 0 10px 0; padding-left: 26px }
table { border-collapse: collapse; width: 100%; margin: 6px 0 12px; font-size: 11.5pt }
th { background: #14202B; color: #fff; text-align: left; padding: 7px 9px; font-weight: 600 }
td { border-bottom: 1px solid #D9DEE3; padding: 6px 9px; vertical-align: top } tr:nth-child(even) td { background: #F5F7F9 }
blockquote { margin: 12px 0; padding: 10px 16px; border-left: 4px solid #3E5C76; background: #EEF2F5; font-size: 12pt; color: #3E5C76 }
code { font-family: Menlo, monospace; font-size: 0.9em; background: #EEF1F4; padding: 1px 5px; border-radius: 3px }
.foot { position: absolute; bottom: 0.22in; left: 0.75in; right: 0.75in; font-size: 9pt; color: #8A97A3; display: flex; justify-content: space-between }
"""

slides = [s.strip().splitlines() for s in re.split(r"\n---\n", open(SRC).read())]
parts = []
for n, s in enumerate(slides, 1):
    cls = "slide title" if n == 1 else "slide"
    foot = "" if n == 1 else f'<div class="foot"><span>RFI Copilot · Summit Ridge Builders (synthetic data) · Databricks</span><span>{n}</span></div>'
    parts.append(f'<section class="{cls}">{block(s)}{foot}</section>')
open(HTML_OUT, "w").write(f"<!DOCTYPE html><html><head><meta charset='utf-8'><style>{CSS}</style></head><body>{''.join(parts)}</body></html>")
subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", f"--print-to-pdf={PDF_OUT}", "file://" + os.path.abspath(HTML_OUT)],
               check=True, capture_output=True)
print("wrote", PDF_OUT, len(slides), "slides")
