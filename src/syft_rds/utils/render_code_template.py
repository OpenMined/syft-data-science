import jinja2
import pkg_resources


def render_data_access_template(filename):
    """
    Render the data access template with the given filename.

    Args:
        filename (str): The name of the file to access in the dataset

    Returns:
        str: The rendered template as a string
    """
    # Get the template path using pkg_resources
    template_path = pkg_resources.resource_filename("syft_rds", "assets")

    # Set up the Jinja environment
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(template_path),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    # Load the template
    template = env.get_template("data_access_template.py.jinja")

    # Render the template with the given filename
    return template.render(filename=filename)
