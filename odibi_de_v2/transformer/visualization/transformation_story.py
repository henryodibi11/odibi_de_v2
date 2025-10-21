"""
================================================================================
📘 transformation_story.py
================================================================================
Visual pipeline playback and documentation utilities for odibi_de_v2.

This module provides lightweight visualization and documentation tools
to help engineers and stakeholders understand how Spark-based transformations
evolve data through each step of a pipeline.

It is designed for **offline review, training, and project handoff** — not for
production execution. Each transformation step captures before/after samples,
schema changes, and explanations, then renders them as an interactive,
scrollable HTML story that can be viewed in Databricks or any web browser.

--------------------------------------------------------------------------------
Key Components
--------------------------------------------------------------------------------
1. **TransformationStory**
   - Captures step-by-step transformation history (before/after snapshots).
   - Generates rich HTML documentation with collapsible sections, schema diffs,
     and optional references (e.g., unit tables, constants).
   - Exports to /dbfs/, ADLS, or local paths for stakeholder review.
   - Designed for visual clarity, not version control.

2. **SparkWorkflowNodePlayback**
   - Lightweight wrapper around SparkWorkflowNode for replaying transformations
     without writing to Delta or invoking trackers.
   - Ideal for documentation, dry runs, and stakeholder-facing demos.
   - Automatically generates a TransformationStory object.

--------------------------------------------------------------------------------
Usage Example
--------------------------------------------------------------------------------
>>> from odibi_de_v2.transformer.visualization.transformation_story import (
...     SparkWorkflowNodePlayback
... )
>>> playback = SparkWorkflowNodePlayback(steps=my_steps)
>>> story = playback.transform(project="Energy Efficiency", layer="Silver", table="nkc_germ_dryer_1")
>>> story.export_html("/dbfs/FileStore/energy_efficiency/nkc_germ_dryer_1_story.html")

--------------------------------------------------------------------------------
Author
--------------------------------------------------------------------------------
Henry Odibi · Ingredion Digital Manufacturing
================================================================================
"""

from odibi_de_v2.spark_utils import get_active_spark
import html, time, pandas as pd, os, textwrap
from IPython.display import display, HTML
from datetime import datetime

# --- Markdown rendering with fallback ---
try:
    import markdown2
    def _render_md(txt: str) -> str:
        """Render GitHub-style Markdown safely with table + code support."""
        clean = textwrap.dedent(txt or "").strip()
        return markdown2.markdown(
            clean,
            extras=[
                "tables",
                "fenced-code-blocks",
                "break-on-newline",
                "strike",
                "task_list",
                "cuddled-lists",
                "code-friendly",
            ],
        )
except Exception:
    import markdown
    def _render_md(txt: str) -> str:
        clean = textwrap.dedent(txt or "").strip()
        return markdown.markdown(clean, extensions=["tables", "fenced_code", "nl2br"])


def _is_databricks():
    """Detect if running inside a Databricks notebook."""
    try:
        import IPython
        shell = IPython.get_ipython()
        return shell and "databricks" in shell.__class__.__module__.lower()
    except:
        return False


class TransformationStory:
    """
    ---------------------------------------------------------------------------
    🧩 TransformationStory
    ---------------------------------------------------------------------------
    Collects and visualizes step-by-step Spark DataFrame transformations.

    Each step captures:
      • Before/after Pandas samples
      • Schema changes (added/dropped columns)
      • Optional written explanations (Markdown-supported)
      • Duration and metadata for transparency

    The final output is an interactive, scrollable HTML report with collapsible
    sections for each step, optional reference tables, and a professional footer
    including author information and confidentiality notice.

    ---------------------------------------------------------------------------
    Purpose
    ---------------------------------------------------------------------------
    • Simplify technical handoff and stakeholder communication
    • Provide explainability and traceability for each transformation
    • Preserve transformation intent without exposing Databricks internals

    ---------------------------------------------------------------------------
    Typical Usage
    ---------------------------------------------------------------------------
    >>> story = TransformationStory(project="Energy Efficiency", layer="Silver", table="nkc_germ_dryer_1")
    >>> story.add(idx=1, name="Load Bronze Data", stype="SQL", before=None, after=df, ...)
    >>> story.export_html("/dbfs/FileStore/energy_efficiency/story.html")

    ---------------------------------------------------------------------------
    Notes
    ---------------------------------------------------------------------------
    • Designed for visualization and storytelling, not production logic.
    • When attached to SparkWorkflowNodePlayback, it builds automatically.
    • References (e.g., unit tables, constants) can be added via `story.references`.
    ---------------------------------------------------------------------------
    """

    def __init__(self, project=None, layer=None, table=None):
        self.project, self.layer, self.table = project, layer, table
        self.steps = []
        self.references = []  # reference DataFrames to display before steps

    def add(self, idx, name, stype, before, after, before_cols, after_cols, explanation=None):
        before_rows = len(before) if isinstance(before, pd.DataFrame) else 0
        after_rows = len(after) if isinstance(after, pd.DataFrame) else 0
        self.steps.append(
            dict(
                idx=idx,
                name=name,
                stype=stype,
                before=before,
                after=after,
                before_cols=before_cols,
                after_cols=after_cols,
                before_rows=before_rows,
                after_rows=after_rows,
                explanation=explanation,
            )
        )

    def add_reference(self, title: str, pdf: pd.DataFrame):
        self.references.append({"title": str(title), "df": pdf})

    # -------------------------------------------------------------------------
    def _describe_step(self, s):
        name = s.get("name", "").lower()
        stype = s.get("stype", "").lower()
        added = [c for c in s["after_cols"] if c not in s["before_cols"]]
        dropped = [c for c in s["before_cols"] if c not in s["after_cols"]]
        if "pivot" in name or "pivot" in stype:
            return f"Pivot operation reshaped data into {len(s['after_cols'])} columns."
        if "unpivot" in name or "unpivot" in stype:
            return "Unpivot operation normalized wide data into long form."
        if "derived" in name or "calc" in name:
            return f"Derived new columns: {', '.join(added) or 'none'}."
        if "join" in name:
            return f"Join operation merged columns: {', '.join(added) or 'none'}."
        if "filter" in name:
            return "Filter applied to refine rows."
        return f"Transformation produced {len(s['after_cols'])} columns."

    # -------------------------------------------------------------------------
    def _build_html(self, max_rows=10, max_cols=None, title="Transformation Story"):
        ts = datetime.now().strftime("%B %d, %Y")
        escape = lambda x: html.escape(str(x))

        parts = [f"""<!DOCTYPE html>
    <html>
    <head>
    <meta charset='utf-8'>
    <title>{escape(title)}</title>
    <style>
        /* Base layout */
        body {{
            background:#0f1117;
            color:#e6e6e6;
            font-family:system-ui, Segoe UI, sans-serif;
            font-size:14px;
            line-height:1.6;
            margin:0;
            padding:16px;
        }}
        h1,h2,h3 {{
            color:#fff;
            margin-top:10px;
            margin-bottom:10px;
        }}

        /* Data tables */
        table {{
            border-collapse:collapse;
            width:max-content;
            font-size:13.5px;
            background:white;
            color:black;
        }}
        td,th {{
            border:1px solid #ccc;
            padding:6px 10px;
            white-space:nowrap;
        }}
        th {{
            background:#f1f1f1;
            position:sticky;
            top:0;
            z-index:1;
        }}

        /* Step cards */
        .step {{
            background:#1a1e27;
            border-radius:8px;
            padding:10px 12px;
            margin:12px 0;
        }}
        .empty {{
            color:#999;
            font-style:italic;
        }}

        /* Scrollable dataframes */
        .story-scroll {{
            overflow-x:auto;
            overflow-y:auto;
            max-width:100%;
            max-height:520px;
            display:block;
            padding-bottom:8px;
            margin-bottom:6px;
            border-bottom:1px solid #333;
            -webkit-overflow-scrolling:touch;
        }}

        /* Collapsibles */
        .collapsible {{
            background:#2b2e39;
            color:#fff;
            cursor:pointer;
            padding:8px 12px;
            border:none;
            text-align:left;
            outline:none;
            font-weight:600;
            border-radius:4px;
            margin-bottom:5px;
            width:100%;
            font-size:14px;
        }}
        .active, .collapsible:hover {{
            background:#3c4354;
        }}
        .content {{
            display:none;
            overflow:hidden;
        }}

        /* Highlights for column changes */
        .added {{
            background:#093009;
            color:#b2fdb2;
        }}
        .dropped {{
            background:#3a0b0b;
            color:#ffb2b2;
        }}

        /* Section headers */
        .summary {{
            background:#1a1e27;
            border-radius:8px;
            padding:10px 12px;
            margin-bottom:15px;
        }}

        /* Explanations (markdown content) */
        .explanation {{
            color:#b0c4ff;
            font-style:italic;
            margin-top:8px;
            line-height:1.7;
            font-size:14px;
        }}
        .explanation p {{
            margin-bottom:8px;
            line-height:1.7;
        }}
        .explanation table {{
            margin-top:10px;
            margin-bottom:10px;
            border-collapse:collapse;
            width:100%;
            background:#1f232b;
            color:#e6e6e6;
            font-size:13.5px;
        }}
        .explanation th, .explanation td {{
            border:1px solid #555;
            padding:8px 10px;
        }}
        .explanation th {{
            background:#2b2e39;
            color:#fff;
            font-size:14px;
            position:sticky;
            top:0;
            z-index:1;
        }}
        .explanation tr:nth-child(even) {{
            background-color:#232832;
        }}
        .explanation code {{
            background:#1f232b;
            color:#b0e0ff;
            padding:2px 5px;
            border-radius:3px;
        }}
        .explanation pre {{
            background:#1f232b;
            color:#b0e0ff;
            padding:8px;
            border-radius:5px;
            overflow-x:auto;
            font-size:13px;
        }}

        /* Scrollbar styling */
        ::-webkit-scrollbar {{
            height:8px;
            width:8px;
            background:#222;
        }}
        ::-webkit-scrollbar-thumb {{
            background:#666;
            border-radius:4px;
        }}

        /* Reference tables */
        .reference {{
            background:#141823;
            border-radius:8px;
            padding:10px 12px;
            margin:10px 0;
        }}

        /* Expand/collapse all buttons */
        .expand-btn {{
            background:#3c4354;
            border:none;
            color:white;
            padding:6px 12px;
            border-radius:5px;
            cursor:pointer;
            margin-bottom:10px;
        }}
        .expand-btn:hover {{
            background:#4a5265;
        }}
    </style>
    <script>
    document.addEventListener('click', function(e){{
        if(e.target.classList.contains('collapsible')){{
            e.target.classList.toggle('active');
            var c = e.target.nextElementSibling;
            c.style.display = c.style.display === 'block' ? 'none' : 'block';
        }}
    }});
    function toggleAll(open) {{
        document.querySelectorAll('.collapsible').forEach(btn => {{
            var c = btn.nextElementSibling;
            if(open) {{
                btn.classList.add('active');
                c.style.display = 'block';
            }} else {{
                btn.classList.remove('active');
                c.style.display = 'none';
            }}
        }});
    }}
    </script>
    </head>
    <body>

    <h1>{escape(title)}</h1>
    <p><b>Project:</b> {escape(self.project or '')} |
    <b>Layer:</b> {escape(self.layer or '')} |
    <b>Table:</b> {escape(self.table or '')}</p>
    <p>🔍 Each step below shows up to {max_rows} sample rows for illustration.</p>
    <button class='expand-btn' onclick='toggleAll(true)'>Expand All</button>
    <button class='expand-btn' onclick='toggleAll(false)'>Collapse All</button>

    <div class='summary'>
        <h2>Transformation Overview</h2>
        <p>This story visually traces how the dataset evolves from raw process
        and reference inputs into the structured Silver-layer output.</p>
    </div>
    """]

        # --------------------------
        # Collapsible References Section
        # --------------------------
        if self.references:
            parts.append("<div class='summary'><h2>References</h2></div>")
            for ref in self.references:
                title_txt = escape(ref.get("title", "Reference"))
                pdf = ref.get("df", None)
                parts.append(
                    f"<div class='reference'>"
                    f"<button class='collapsible'>{title_txt}</button>"
                    f"<div class='content'>"
                )
                if isinstance(pdf, pd.DataFrame) and not pdf.empty:
                    pd.set_option("display.float_format", lambda x: f"{x:.6f}")
                    table_html = pdf.to_html(index=False, border=0, float_format="%.6f")
                    pd.reset_option("display.float_format")
                    parts.append(f"<div class='story-scroll'>{table_html}</div>")
                else:
                    parts.append("<div class='empty'>∅ No reference data</div>")
                parts.append("</div></div>")

        # ---------------------------------------------------------------------
        # Transformation Steps
        # ---------------------------------------------------------------------
        parts.append("<div class='summary'><h2>Transformation Steps</h2></div>")
        for s in self.steps:
            added = [c for c in s["after_cols"] if c not in s["before_cols"]]
            dropped = [c for c in s["before_cols"] if c not in s["after_cols"]]
            summary_txt = "" if s.get("explanation") else self._describe_step(s)
            flag = ""

            name_lower = s["name"].lower()
            ignore_keywords = ["unpivot", "unit", "lookup"]
            if not any(k in name_lower for k in ignore_keywords):
                before_cols = set(s["before_cols"])
                after_cols = set(s["after_cols"])
                if before_cols and (len(dropped) / len(before_cols)) > 0.3:
                    flag = (
                        f"<p style='color:#ff6b6b;'>⚠️ Notice: {len(dropped)} columns were "
                        f"dropped ({len(before_cols)} → {len(after_cols)}).</p>"
                    )

            clean_name = s["name"].replace("_", " ").title()
            if s["idx"] == 1 and clean_name.lower() == "wrapper":
                clean_name = "Get Data"

            parts.append(
                f"<div class='step'>"
                f"<button class='collapsible'>Step {s['idx']}: {escape(clean_name)}</button>"
                f"<div class='content'>"
            )

            # ✅ Markdown rendering — no escaping
            if s.get("explanation"):
                import markdown2, textwrap
                explanation_html = markdown2.markdown(
                    textwrap.dedent(s["explanation"]),
                    extras=["tables", "fenced-code-blocks", "break-on-newline"],
                    safe_mode=False
                )
                parts.append(f"<div class='explanation'>{explanation_html}</div>")
            elif summary_txt:
                parts.append(f"<p>{escape(summary_txt)}</p>")

            if flag:
                parts.append(flag)
            parts.append(
                f"<p>Added: {', '.join(added) or '—'} | Dropped: {', '.join(dropped) or '—'}</p>"
            )

            for lbl, df in [("Before", s["before"]), ("After", s["after"])]:
                parts.append(f"<b>{lbl}</b><br>")
                if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    parts.append("<div class='empty'>∅ No sample</div>")
                else:
                    highlight_cols = {
                        c: 'added' if c in added else 'dropped' if c in dropped else ''
                        for c in df.columns
                    }
                    visible_df = (
                        df.iloc[:, :max_cols] if (max_cols and df.shape[1] > max_cols) else df
                    )
                    table_html = visible_df.head(max_rows).to_html(index=False, border=0)
                    for col, cls in highlight_cols.items():
                        if cls:
                            table_html = table_html.replace(f">{col}<", f" class='{cls}'>{col}<")
                    parts.append(f"<div class='story-scroll'>{table_html}</div>")
                    if max_cols and df.shape[1] > max_cols:
                        parts.append(f"<div class='empty'>… +{df.shape[1]-max_cols} more columns</div>")
            parts.append("</div></div>")

        # ---------------------------------------------------------------------
        # Professional Footer
        # ---------------------------------------------------------------------
        footer = f"""
    <hr style='border:1px solid #333;margin-top:20px;'>
    <div style='font-size:12px;color:#bbb;text-align:center;line-height:1.6;'>
        <p>
            <b>Created by:</b> Henry Odibi
            <img src="https://flagcdn.com/w20/ng.png" alt="🇳🇬"
                style="height:14px;vertical-align:text-bottom;margin-left:4px;">
            &nbsp;&nbsp;|&nbsp;&nbsp;
            <b>Version:</b> 1.0
        </p>
        <p><b>Generated on:</b> {ts}</p>
        <p>Ingredion Digital Manufacturing - Confidential.<br>
        Unauthorized distribution or reproduction of this document is prohibited.</p>
        <p style='color:#8fd3ff;font-style:italic;margin-top:8px;'>
            “Data is a representation of reality and data engineering is the discipline of preserving that truth through systems.” — Henry Odibi
        </p>
    </div>
    </body></html>
    """
        parts.append(footer)
        return "\n".join(parts)


    # -------------------------------------------------------------------------
    def export_html(self, output_path, max_rows=10, max_cols=None, title=None):
        env_databricks = _is_databricks()
        html_str = self._build_html(max_rows=max_rows, max_cols=max_cols, title=title or "Transformation Story")
        try:
            if output_path.startswith("/dbfs/"):
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(html_str)
                if env_databricks:
                    link = output_path.replace("/dbfs/FileStore", "/files").replace(" ", "%20")
                    workspace = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
                    full_url = f"{workspace}{link}" if workspace else link
                    display(HTML(f"<h3>✅ Story created successfully!</h3><a href='{full_url}' target='_blank'>🔗 Open Story</a>"))
                else:
                    print(f"✅ HTML saved to: {output_path}")
            elif output_path.startswith("abfss:"):
                dbutils.fs.put(output_path, html_str, overwrite=True)  # type: ignore # noqa
                print(f"✅ Uploaded to ADLS: {output_path}")
            else:
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(html_str)
                print(f"✅ Saved to local path: {output_path}")
        except Exception as e:
            print(f"[WARN] Export failed: {e}")

    def display_html(self, max_rows=10, max_cols=None):
        html_str = self._build_html(max_rows=max_rows, max_cols=max_cols)
        display(HTML(html_str))



class SparkWorkflowNodePlayback:
    """
    ---------------------------------------------------------------------------
    ⚙️ SparkWorkflowNodePlayback
    ---------------------------------------------------------------------------
    Lightweight “replay engine” for SparkWorkflowNode transformations.

    This class executes a list of transformation steps (SQL, config, or Python)
    in sequence, mirroring the behavior of SparkWorkflowNode but without
    writing to Delta tables or invoking trackers.

    Instead of persisting data, it captures lightweight before/after samples and
    schema information to generate a **TransformationStory** object that can be
    visualized or exported as HTML.

    ---------------------------------------------------------------------------
    Key Features
    ---------------------------------------------------------------------------
    • Replays config-driven pipelines safely in read-only mode  
    • Supports SQL, config, callable, and tuple-based transformation steps  
    • Collects sample data for before/after comparison  
    • Integrates automatically with TransformationStory for visualization

    ---------------------------------------------------------------------------
    Example
    ---------------------------------------------------------------------------
    >>> playback = SparkWorkflowNodePlayback(steps=my_steps)
    >>> story = playback.transform(project="Energy Efficiency", layer="Silver", table="nkc_germ_dryer_1")
    >>> story.export_html("/dbfs/FileStore/energy_efficiency/story.html")

    ---------------------------------------------------------------------------
    Notes
    ---------------------------------------------------------------------------
    • This utility is intended for documentation and testing, not production ETL.
    • Step order is auto-detected; step_explanations can be optionally passed
      for richer documentation output.
    ---------------------------------------------------------------------------
    """
    def __init__(self, steps):
        from odibi_de_v2.transformer import SparkWorkflowNode
        self.SparkWorkflowNode = SparkWorkflowNode  # keep reference for refs too
        self.node = SparkWorkflowNode(steps=steps, register_view=False)

    def _run_steps_return_df(self, steps):
        """NEW: Run a mini SparkWorkflowNode-compatible step list and return final DF."""
        spark = get_active_spark()
        df = None
        for step in steps:
            if isinstance(step, dict) and "step_type" in step:
                stype = step["step_type"]
                step_val = step["step_value"]
                params = step.get("params", {})
                if stype == "sql":
                    df = spark.sql(step_val)
                elif stype == "config":
                    df = self.node._apply_config_op(df, step_val, params)
                elif stype == "python":
                    func = self.node._load_function(step_val)
                    df = func(df, **params)
            elif isinstance(step, str):
                df = spark.sql(step)
            elif callable(step):
                df = step(df)
            elif isinstance(step, tuple):
                if len(step) == 2:
                    func, params = step
                    if isinstance(func, str):
                        func = self.node._load_function(func)
                    df = func(df, **(params or {}))
                elif len(step) == 1:
                    func = step[0]
                    if isinstance(func, str):
                        func = self.node._load_function(func)
                    df = func(df)
                else:
                    raise ValueError(f"Unsupported tuple length {len(step)} in references")
            else:
                raise ValueError(f"Unsupported step type in references: {type(step)}")
        return df

    def transform(self, project=None, layer=None, table=None, sample_rows=10, step_explanations=None,
                  references=None, reference_sample_rows=1000):
        """
        references: list of items, each item can be:
          - {"name": "...", "steps": [ ... SparkWorkflowNode-style steps ... ]}
          - {"name": "...", "sql": "SELECT ..."}  # executed directly
          - {"name": "...", "df": <Spark DataFrame>}  # taken as-is
        """
        spark = get_active_spark()
        df = None
        story = TransformationStory(project, layer, table)
        step_explanations = step_explanations or []

        ordered_steps = sorted(
            [(s.get("step_order", i), s) if isinstance(s, dict) else (i, s)
             for i, s in enumerate(self.node.steps)],
            key=lambda x: x[0]
        )

        for idx, step in enumerate([s for _, s in ordered_steps], start=1):
            before_cols = df.columns if df is not None else []
            before = df.limit(sample_rows).toPandas() if df is not None else None

            name, stype = "Unknown", "unknown"
            try:
                if isinstance(step, dict) and "step_type" in step:
                    stype = step["step_type"]
                    step_val = step["step_value"]
                    params = step.get("params", {})
                    name = step_val
                    if stype == "sql":
                        df = spark.sql(step_val)
                    elif stype == "config":
                        df = self.node._apply_config_op(df, step_val, params)
                    elif stype == "python":
                        func = self.node._load_function(step_val)
                        df = func(df, **params)
                elif isinstance(step, str):
                    stype = "sql"; name = "SQL"; df = spark.sql(step)
                elif callable(step):
                    stype = "callable"; name = step.__name__; df = step(df)
                elif isinstance(step, tuple):
                    if len(step) == 2:
                        func, params = step
                        if isinstance(func, str):
                            func = self.node._load_function(func)
                        stype = "callable"; name = getattr(func, "__name__", str(func))
                        df = func(df, **(params or {}))
                    elif len(step) == 1:
                        func = step[0]
                        if isinstance(func, str):
                            func = self.node._load_function(func)
                        stype = "callable"; name = getattr(func, "__name__", str(func))
                        df = func(df)
                    else:
                        raise ValueError(f"Unsupported tuple length {len(step)} for step {idx}")
            except Exception as e:
                print(f"[WARN] Step {idx} ({name}) failed: {e}")

            after_cols = df.columns if df is not None else []
            after = df.limit(sample_rows).toPandas() if df is not None else None
            explanation = step_explanations[idx - 1] if idx - 1 < len(step_explanations) else None

            story.add(idx, name, stype, before, after, before_cols, after_cols, explanation)

        # --------------------------
        # NEW: Build references via SparkWorkflowNode / SQL / DF
        # --------------------------
        if references:
            for ref in references:
                try:
                    title = ref.get("name", "Reference")
                    pdf = None
                    if "steps" in ref:
                        rdf = self._run_steps_return_df(ref["steps"])
                        if rdf is not None:
                            pdf = rdf.limit(reference_sample_rows).toPandas()
                    elif "sql" in ref:
                        rdf = spark.sql(ref["sql"])
                        pdf = rdf.limit(reference_sample_rows).toPandas()
                    elif "df" in ref:
                        rdf = ref["df"]
                        if hasattr(rdf, "limit"):
                            pdf = rdf.limit(reference_sample_rows).toPandas()
                        elif isinstance(rdf, pd.DataFrame):
                            pdf = rdf  # already pandas
                    if isinstance(pdf, pd.DataFrame):
                        story.add_reference(title, pdf)
                    else:
                        story.add_reference(title, pd.DataFrame())  # empty placeholder
                except Exception as e:
                    print(f"[WARN] Reference '{ref.get('name','Reference')}' failed: {e}")
                    story.add_reference(ref.get("name", "Reference"), pd.DataFrame())

        return story
