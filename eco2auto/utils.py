"""rich terminal, loguru utils."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar

import rich
import tqdm.rich
import whenever
from loguru import logger
from rich.logging import RichHandler
from rich.theme import Theme
from tqdm import TqdmExperimentalWarning

if TYPE_CHECKING:
    from logging import LogRecord


__all__ = ['LogHandler']

console = rich.get_console()
console.push_theme(Theme({'logging.level.success': 'bold blue'}))


class LogHandler(RichHandler):
    _NEW_LEVELS: ClassVar[dict[int, str]] = {5: 'TRACE', 25: 'SUCCESS'}

    def emit(self, record: LogRecord) -> None:
        if name := self._NEW_LEVELS.get(record.levelno):
            record.levelname = name

        return super().emit(record)

    @classmethod
    def set(
        cls,
        level: int | str = 20,
        *,
        rich_tracebacks: bool = False,
        remove: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        `loguru.logger` 세팅.

        Parameters
        ----------
        level : int | str, optional
        rich_tracebacks : bool, optional
        remove : bool, optional

        Examples
        --------
        >>> LogHandler.set(20)
        >>> from loguru import logger
        >>> logger.debug('debug')
        >>> logger.info('info')  # doctest: +ELLIPSIS
        [...] INFO ...
        >>> logger.success('success')  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
              SUCCESS ...
        """
        handler = cls(
            console=console,
            markup=True,
            log_time_format='[%X]',
            rich_tracebacks=rich_tracebacks,
        )

        if remove:
            logger.remove()

        logger.add(handler, level=level, format='{message}', **kwargs)


class Tqdm(tqdm.rich.tqdm):
    def __init__(self, iterable, total=None, initial=None, miniters=0, **kwargs):
        warnings.simplefilter('ignore', TqdmExperimentalWarning)
        super().__init__(
            iterable=iterable,
            total=total,
            initial=initial,
            miniters=miniters,
            **kwargs,
        )
        self.display()

    def log(self):
        d = self.format_dict
        if not (rate := d['rate']):
            logger.info('ETA Unknown | Speed Unknown')
            return

        remaining = (d['total'] - d['n']) / rate
        eta = (
            (whenever.SystemDateTime.now() + whenever.TimeDelta(seconds=remaining))
            .replace(nanosecond=0)
            .format_common_iso()
            .replace('T', ' ')
        )
        logger.info(
            'ETA {} | Speed {:.4g} it/min ({:.4g} sec/it)', eta, rate * 60, 1.0 / rate
        )
