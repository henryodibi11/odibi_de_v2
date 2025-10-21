def generate_transformation_story(
    steps,
    project,
    layer,
    table,
    output_path,
    step_explanations=None,
    references=None,
    max_rows=10,
    max_cols=None,
    reference_sample_rows=5000
):
    from odibi_de_v2.transformer import SparkWorkflowNodePlayback
    """
    One-line utility to generate, export, and return a clickable HTML Story.
    Now supports optional reference tables rendered before transformation steps.
    """
    playback = SparkWorkflowNodePlayback(steps)
    story = playback.transform(
        project=project,
        layer=layer,
        table=table,
        sample_rows=max_rows,
        step_explanations=step_explanations,
        references=references,
        reference_sample_rows=reference_sample_rows
    )
    return story.export_html(output_path, max_rows=max_rows, max_cols=max_cols)
