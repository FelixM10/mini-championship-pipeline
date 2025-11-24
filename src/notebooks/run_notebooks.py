import subprocess
import shutil
from pathlib import Path


def main():
    """
    Execute notebooks in ROOT/notebooks/ and export reports into ROOT/reports.

    Steps per notebook:
      - Execute with nbconvert and export to HTML, hiding code cells (--no-input).
      - If wkhtmltopdf is available, convert HTML → PDF.
      - Otherwise, only HTML reports are generated.

    Python requirements:
      - jupyter
      - nbconvert

    System requirement for PDF export (optional):
      - wkhtmltopdf (https://wkhtmltopdf.org/downloads.html)
    """
    script_dir = Path(__file__).resolve().parent        # ROOT/src/notebooks
    project_root = script_dir.parents[1]                # ROOT/

    notebooks_dir = project_root / "notebooks"
    reports_dir = project_root / "reports"

    reports_dir.mkdir(parents=True, exist_ok=True)

    if not notebooks_dir.exists():
        print(f"Notebooks directory not found: {notebooks_dir}")
        return

    notebooks = sorted(notebooks_dir.glob("*.ipynb"))
    if not notebooks:
        print(f"No .ipynb files found in {notebooks_dir}.")
        return

    has_wkhtmltopdf = shutil.which("wkhtmltopdf") is not None
    if not has_wkhtmltopdf:
        print("wkhtmltopdf not installed; generating HTML reports only.")

    for nb in notebooks:
        print(f"Executing notebook: {nb.name}")

        html_output = reports_dir / f"{nb.stem}.html"

        # Step 1 — Execute notebook and export to HTML with no code cells
        try:
            subprocess.run(
                [
                    "jupyter",
                    "nbconvert",
                    "--to", "html",
                    "--execute",
                    "--no-input",              # hide code cells, keep outputs/markdown
                    "--output", html_output.name,
                    "--output-dir", str(reports_dir),
                    str(nb),
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Failed to execute/export {nb.name} to HTML: {e}")
            continue

        print(f"  → HTML report written: {html_output}")

        # Step 2 — Optionally convert HTML → PDF via wkhtmltopdf
        if has_wkhtmltopdf:
            pdf_output = reports_dir / f"{nb.stem}.pdf"
            print(f"  Converting HTML to PDF: {pdf_output.name}")

            try:
                subprocess.run(
                    [
                        "wkhtmltopdf",
                        str(html_output),
                        str(pdf_output),
                    ],
                    check=True,
                )
                print(f"  → PDF report written: {pdf_output}")
            except subprocess.CalledProcessError as e:
                print(f"  Failed to convert {html_output} to PDF: {e}")

    print("Notebook report generation complete.")


if __name__ == "__main__":
    main()
