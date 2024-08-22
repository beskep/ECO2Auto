from logging import LogRecord
from pathlib import Path
from typing import ClassVar

import rich
from loguru import logger
from rich import progress
from rich.logging import RichHandler
from rich.theme import Theme

console = rich.get_console()
console.push_theme(Theme({'logging.level.success': 'bold blue'}))


class _RichHandler(RichHandler):
    _NEW_LVLS: ClassVar[dict[int, str]] = {5: 'TRACE', 25: 'SUCCESS'}

    def emit(self, record: LogRecord) -> None:
        if name := self._NEW_LVLS.get(record.levelno, None):
            record.levelname = name

        return super().emit(record)


def set_logger(
    level: int | str = 20,
    *,
    file: str | Path | None = 'eco2auto.log',
    rich_tracebacks: bool = False,
    rotation='1 month',
    retention='1 year',
    **kwargs,
):
    handler = _RichHandler(
        console=console,
        markup=True,
        log_time_format='[%X]',
        rich_tracebacks=rich_tracebacks,
    )

    logger.remove()
    logger.add(handler, level=level, format='{message}', **kwargs)

    if file:
        logger.add(
            file,
            level=min(10, level),
            rotation=rotation,
            retention=retention,
            encoding='UTF-8-SIG',
        )


class Progress(progress.Progress):
    @classmethod
    def get_default_columns(cls) -> tuple[progress.ProgressColumn, ...]:
        return (
            progress.TextColumn('[progress.description]{task.description}'),
            progress.BarColumn(bar_width=60),
            progress.MofNCompleteColumn(),
            progress.TaskProgressColumn(show_speed=True),
            progress.TimeRemainingColumn(elapsed_when_finished=True),
        )
