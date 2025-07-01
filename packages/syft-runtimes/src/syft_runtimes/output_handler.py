from pathlib import Path
from typing import Protocol

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner

from syft_runtimes.models import JobConfig


class JobOutputHandler(Protocol):
    """Protocol defining the interface for job output handling and display"""

    def on_job_start(self, job_config: JobConfig) -> None:
        """Display job configuration"""
        pass

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        """Display job progress"""
        pass

    def on_job_completion(self, return_code: int) -> None:
        """Display job completion status"""
        pass


class FileOutputHandler(JobOutputHandler):
    """Handles writing job output to log files"""

    def __init__(self):
        pass

    def on_job_start(self, job_config: JobConfig) -> None:
        self.config = job_config
        self.stdout_file = (job_config.logs_dir / "stdout.log").open("w")
        self.stderr_file = (job_config.logs_dir / "stderr.log").open("w")
        self.on_job_progress(stdout="Starting job...\n", stderr="Starting job...\n")

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        if stdout:
            self.stdout_file.write(stdout)
            self.stdout_file.flush()
        if stderr:
            self.stderr_file.write(stderr)
            self.stderr_file.flush()

    def on_job_completion(self, return_code: int) -> None:
        self.on_job_progress(
            stdout=f"Job completed with return code {return_code}\n",
            stderr=f"Job completed with return code {return_code}\n",
        )
        self.close()

    def close(self) -> None:
        self.stdout_file.close()
        self.stderr_file.close()


class RichConsoleUI(JobOutputHandler):
    """Rich console implementation of JobOutputHandler"""

    def __init__(self, show_stdout: bool = True, show_stderr: bool = True):
        self.show_stdout = show_stdout
        self.show_stderr = show_stderr
        self.console = Console()
        spinner = Spinner("dots")
        self.live = Live(spinner, refresh_per_second=10)

    def on_job_start(self, job_config: JobConfig) -> None:
        self.console.print(
            Panel.fit(
                "\n".join(
                    [
                        "[bold green]Starting job[/]",
                        f"[bold white]Execution:[/] [cyan]{' '.join(job_config.runtime.cmd)} {' '.join(job_config.args)}[/]",
                        f"[bold white]Dataset Dir.:[/]  [cyan]{_limit_path_depth(job_config.data_path)}[/]",
                        f"[bold white]Output Dir.:[/]   [cyan]{_limit_path_depth(job_config.output_dir)}[/]",
                        f"[bold white]Timeout:[/]  [cyan]{job_config.timeout}s[/]",
                    ]
                ),
                title="[bold]Job Configuration",
                border_style="cyan",
            )
        )
        try:
            self.live.start()
            self.live.console.print("[bold cyan]Running job...[/]")
        except Exception as e:
            self.console.print(f"[red]Error starting live: {e}[/]")

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        # Update UI display
        if not self.live:
            return

        if stdout and self.show_stdout:
            self.live.console.print(stdout, end="")
        if stderr and self.show_stderr:
            self.live.console.print(f"[red]{stderr}[/]", end="")

    def on_job_completion(self, return_code: int) -> None:
        # Update UI display
        if self.live:
            self.live.stop()

        if return_code == 0:
            self.console.print("\n[bold green]Job completed successfully![/]")
        else:
            self.console.print(
                f"\n[bold red]Job failed with return code {return_code}[/]"
            )

    def __del__(self):
        self.live.stop()


class TextUI(JobOutputHandler):
    """Simple text-based implementation of JobOutputHandler using print statements"""

    def __init__(self, show_stdout: bool = True, show_stderr: bool = True):
        self.show_stdout = show_stdout
        self.show_stderr = show_stderr
        self._job_running = False

    def on_job_start(self, config: JobConfig) -> None:
        first_line = "================ Job Configuration ================"
        last_line = "=" * len(first_line)
        print(f"\n{first_line}")
        print(f"Execution:    {' '.join(config.runtime.cmd)} {' '.join(config.args)}")
        print(f"Dataset Dir.: {_limit_path_depth(config.data_path)}")
        print(f"Output Dir.:  {_limit_path_depth(config.output_dir)}")
        print(f"Timeout:      {config.timeout}s")
        print(f"{last_line}\n")
        print("[STARTING JOB]")
        self._job_running = True

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        if not self._job_running:
            return
        if stdout and self.show_stdout:
            print(stdout, end="")
        if stderr and self.show_stderr:
            print(f"[STDERR] {stderr}", end="")

    def on_job_completion(self, return_code: int) -> None:
        self._job_running = False
        if return_code == 0:
            print("\n[JOB COMPLETED SUCCESSFULLY]\n")
        else:
            print(f"\n[JOB FAILED] Return code: {return_code}\n")

    def __del__(self):
        self._job_running = False


# Helper function to limit path depth
def _limit_path_depth(path: Path, max_depth: int = 4) -> str:
    parts = path.parts
    if len(parts) <= max_depth:
        return str(path)
    return str(Path("...") / Path(*parts[-max_depth:]))
