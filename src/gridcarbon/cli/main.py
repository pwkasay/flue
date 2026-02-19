"""CLI for gridcarbon.

Commands:
    gridcarbon now          Get current carbon intensity
    gridcarbon forecast     Get 24-hour forecast
    gridcarbon seed         Seed historical data from NYISO + weather
    gridcarbon ingest       Run continuous ingestion (NYISO + weather)
    gridcarbon serve        Start the FastAPI server
    gridcarbon status       Show database status
"""

import asyncio
import logging
import re
from datetime import date, timedelta

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel

app = typer.Typer(
    name="gridcarbon",
    help="Real-time carbon intensity tracking and forecasting for the NYISO grid.",
    no_args_is_help=True,
)
console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)-8s %(name)s: %(message)s",
    )


def _redact_dsn(dsn: str) -> str:
    """Redact password from DSN for display."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", dsn)


def _print_pipeline_result(result, label: str = "") -> None:
    """Display a PipelineResult's metrics in a consistent format."""
    prefix = f"  [{label}] " if label else "  "
    console.print(f"{prefix}Pipeline: {result.pipeline_name}")
    console.print(f"{prefix}Duration: {result.duration_seconds:.1f}s")
    console.print(f"{prefix}Dead letters: {result.dead_letters}")
    for sm in result.stage_metrics:
        p50 = sm.get("latency_p50")
        lat = f"{p50 * 1000:.1f}ms" if p50 is not None else "n/a"
        console.print(
            f"{prefix}{sm['stage']}: {sm['items_out']}/{sm['items_in']} ok, "
            f"{sm['items_errored']} errors, p50={lat}"
        )


@app.command()
def now(verbose: bool = typer.Option(False, "--verbose", "-v")) -> None:
    """Get the current grid carbon intensity and recommendation."""
    _setup_logging(verbose)

    async def _run() -> None:
        from ..sources.nyiso import fetch_latest
        from ..storage.store import Store

        with console.status("[bold green]Fetching live data from NYISO..."):
            latest = await fetch_latest()

        if latest is None:
            console.print("[red]Could not fetch current data from NYISO.[/red]")
            raise typer.Exit(1)

        # Save to store
        try:
            with Store() as store:
                store.save_fuel_mix(latest)
        except Exception:
            pass

        ci = latest.carbon_intensity

        # Display
        console.print()
        console.print(
            Panel(
                f"[bold]{ci.category_label}[/bold]\n\n"
                f"[bold]{ci.grams_co2_per_kwh:.0f}[/bold] gCO2/kWh\n\n"
                f"{ci.recommendation}\n\n"
                f"[dim]{latest.timestamp.strftime('%Y-%m-%d %H:%M %Z')}[/dim]",
                title="NYISO Grid Carbon Intensity",
                border_style="green"
                if ci.category in ("very_clean", "clean")
                else "yellow"
                if ci.category == "moderate"
                else "red",
            )
        )

        # Fuel breakdown table
        table = Table(title="Fuel Mix", show_header=True, header_style="bold")
        table.add_column("Fuel", style="cyan")
        table.add_column("MW", justify="right")
        table.add_column("%", justify="right")
        table.add_column("", justify="center")

        for fuel_name, mw in latest.fuel_breakdown.items():
            pct = (mw / latest.total_generation_mw * 100) if latest.total_generation_mw > 0 else 0
            bar = "\u2588" * int(pct / 3)
            table.add_row(fuel_name, f"{mw:,.0f}", f"{pct:.1f}%", bar)

        table.add_section()
        table.add_row(
            "[bold]Total[/bold]",
            f"[bold]{latest.total_generation_mw:,.0f}[/bold]",
            "[bold]100%[/bold]",
            "",
        )
        console.print(table)
        console.print(f"\n  Clean energy: [green]{latest.clean_percentage:.1f}%[/green]")

    asyncio.run(_run())


@app.command()
def forecast(
    hours: int = typer.Option(24, "--hours", "-h", help="Hours to forecast"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Get a carbon intensity forecast with best/worst times."""
    _setup_logging(verbose)

    async def _run() -> None:
        from ..sources.nyiso import fetch_latest
        from ..sources.weather import fetch_forecast as fetch_weather
        from ..forecaster.heuristic import HeuristicForecaster
        from ..storage.store import Store

        with Store() as store:
            forecaster = HeuristicForecaster(store)

            with console.status("[bold green]Building forecast..."):
                # Get current CI
                current_ci = None
                try:
                    latest = await fetch_latest()
                    if latest:
                        current_ci = latest.carbon_intensity
                except Exception:
                    pass

                # Get weather
                weather = None
                try:
                    weather = await fetch_weather(days=2)
                except Exception:
                    pass

                fc = forecaster.forecast(
                    hours=hours,
                    weather=weather,
                    current_intensity=current_ci,
                )

            console.print()
            console.print(Panel(fc.summary, title="Grid Carbon Forecast", border_style="blue"))

            # Hourly table
            table = Table(title=f"\n{hours}-Hour Forecast", show_header=True, header_style="bold")
            table.add_column("Time", style="cyan")
            table.add_column("gCO2/kWh", justify="right")
            table.add_column("Level", justify="center")
            table.add_column("Confidence", justify="center")
            table.add_column("", justify="left")

            for h in fc.hourly:
                ci = h.predicted_intensity
                g = ci.grams_co2_per_kwh
                bar_len = int(g / 20)
                color = (
                    "green"
                    if ci.category in ("very_clean", "clean")
                    else "yellow"
                    if ci.category == "moderate"
                    else "red"
                )
                bar = f"[{color}]{'\u2588' * bar_len}[/{color}]"
                table.add_row(
                    h.hour.strftime("%a %I:%M %p"),
                    f"{g:.0f}",
                    ci.category_label,
                    h.confidence,
                    bar,
                )

    asyncio.run(_run())


@app.command()
def seed(
    days: int = typer.Option(30, "--days", "-d", help="Days of history to seed"),
    no_weather: bool = typer.Option(False, "--no-weather", help="Skip weather data seeding"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Seed historical data from NYISO and Open-Meteo weather."""
    _setup_logging(verbose)

    async def _run() -> None:
        from ..pipeline.ingest import run_seed
        from ..storage.async_store import AsyncStore

        async_store = await AsyncStore.create()
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        sources = "NYISO" if no_weather else "NYISO + weather"
        console.print(
            f"\nSeeding {days} days of {sources} data "
            f"({start_date.isoformat()} \u2192 {end_date.isoformat()})\n"
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Fetching...", total=None)

            def on_progress(day: date, count: int) -> None:
                progress.update(
                    task,
                    description=f"[green]{day.isoformat()}[/green] \u2014 {count} records",
                )

            nyiso_result, weather_result = await run_seed(
                async_store,
                start_date,
                end_date,
                progress_callback=on_progress,
                include_weather=not no_weather,
            )

        console.print("\n[bold green]Seeding complete![/bold green]")
        _print_pipeline_result(nyiso_result, "NYISO")
        if weather_result:
            _print_pipeline_result(weather_result, "Weather")
        console.print(f"  Database: {_redact_dsn(async_store.dsn)}")

        await async_store.close()

    asyncio.run(_run())


@app.command()
def ingest(
    interval: int = typer.Option(300, "--interval", "-i", help="NYISO poll interval in seconds"),
    weather_interval: int = typer.Option(
        3600, "--weather-interval", help="Weather poll interval in seconds"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run continuous data ingestion from NYISO and Open-Meteo weather."""
    _setup_logging(verbose)

    async def _run() -> None:
        from ..pipeline.ingest import run_continuous
        from ..storage.async_store import AsyncStore

        async_store = await AsyncStore.create()
        console.print(
            f"[bold green]Starting continuous ingestion[/bold green]\n"
            f"  NYISO:   polling every {interval}s\n"
            f"  Weather: polling every {weather_interval}s\n"
            f"weir handles graceful shutdown \u2014 press Ctrl+C to stop.\n"
        )
        try:
            nyiso_result, weather_result = await run_continuous(
                async_store,
                poll_interval_seconds=interval,
                weather_poll_interval_seconds=weather_interval,
            )
            console.print("\n[yellow]Ingestion stopped.[/yellow]")
            _print_pipeline_result(nyiso_result, "NYISO")
            _print_pipeline_result(weather_result, "Weather")
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted.[/yellow]")
        finally:
            await async_store.close()

    asyncio.run(_run())


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port", "-p"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Start the FastAPI server."""
    _setup_logging(verbose)
    import uvicorn

    console.print(f"\n[bold green]Starting gridcarbon API[/bold green] at http://{host}:{port}\n")
    uvicorn.run(
        "gridcarbon.api.app:app",
        host=host,
        port=port,
        reload=verbose,
    )


@app.command()
def status(verbose: bool = typer.Option(False, "--verbose", "-v")) -> None:
    """Show database status and data coverage."""
    _setup_logging(verbose)
    from ..storage.store import Store

    with Store() as store:
        count = store.record_count()
        earliest, latest = store.date_range()

        console.print(
            Panel(
                f"Database: {_redact_dsn(store.dsn)}\n"
                f"Records: {count:,}\n"
                f"Earliest: {earliest or 'N/A'}\n"
                f"Latest: {latest or 'N/A'}",
                title="gridcarbon Status",
                border_style="blue",
            )
        )

        if count == 0:
            console.print(
                "\n[yellow]No data yet.[/yellow] Run [bold]gridcarbon seed --days 30[/bold] "
                "to get started.\n"
            )


if __name__ == "__main__":
    app()
