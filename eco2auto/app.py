from __future__ import annotations

import dataclasses as dc
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING, Annotated, Literal

import cyclopts
import polars as pl
from cyclopts import Parameter
from loguru import logger

from eco2auto.automate import BatchRunner, Overwrite, Report  # noqa: TC001
from eco2auto.report import Eco2GraphReport
from eco2auto.utils import LogHandler

if TYPE_CHECKING:
    from collections.abc import Iterable


def _read_reports(paths: Iterable[Path]):
    return pl.concat(
        Eco2GraphReport(p)
        .data()
        .select(
            pl.lit(p.as_posix()).alias('path'),
            pl.lit(p.stem).alias('case'),
            pl.all(),
        )
        for p in paths
    )


app = cyclopts.App(help_format='markdown', help_on_error=True)
app.meta.group_parameters = cyclopts.Group('Option', sort_key=0)


@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    loglevel: Annotated[str | int, Parameter(name=['--loglevel', '-l'])] = 20,
):
    LogHandler.set(level=loglevel)
    app(tokens)


@Parameter(name='*')
@dc.dataclass
class Runner(BatchRunner):
    src: Path
    """대상 경로. ECO2 파일 또는 ECO2 파일이 저장된 폴더 경로."""

    dst: Path | None = None
    """결과 저장 경로. 미지정 시 `src`와 같은 폴더."""

    _: dc.KW_ONLY

    report: Report | Literal['auto'] = 'auto'
    r"""저장 파일.
    `auto`인 경우 eco 파일은 '결과그래프', tpl 파일은 '계산결과' 저장.\
    결과그래프 (`graph`), 계산결과 (`calculations`),
    인증평가서 (`certification`), 업로드 양식 (`upload`) 중 선택.
    """

    extension: Literal['eco', 'tpl', 'any'] = 'any'
    """대상 ECO2 파일 확장자 (ecox, tplx 포함)."""

    overwrite: Overwrite = 'skip'
    """결과 파일이 이미 존재하는 경우
    오류 발생 (`raise`), 덮어쓰기 (`overwrite`), 또는 넘기기 (`skip`).
    """

    restart: int = 0
    """0이 아닌 경우 `restart`회마다 ECO2를 재시작."""

    retry: int = 100
    """오류 발생 시 최대 재시도 횟수."""

    recursive: bool = True
    """`src` 경로에서 ECO2 파일 재귀적 탐색 여부."""


@app.command
def run(runner: Runner):
    """ECO2 자동 평가 및 결과 저장."""
    runner.run()


@app.command
def report(src: Path, dst: Path | None = None):
    """
    다수의 ECO2 결과 파일을 읽고 엑셀 파일로 저장.

    Parameters
    ----------
    src : Path
        대상 폴더. `src`에 위치한 `.xls` 파일을 전부 읽음.
    dst : Path | None
        저장 경로. 미지정 시 `src` 폴더 아래 `Report.xlsx` 파일 저장.

    Raises
    ------
    NotADirectoryError
        `src`가 directory가 아닐 때.
    FileNotFoundError
        `src` 아래 `.xls` 파일이 없을 때.
    FileExistsError
        저장 경로와 같은 파일이 이미 존재할 때.
    """
    if not src.is_dir():
        raise NotADirectoryError(src)
    if not (paths := list(src.glob('*.xls'))):
        msg = f'다음 경로에서 결과 파일을 찾지 못했습니다: {src}'
        raise FileNotFoundError(msg)

    dst = dst or src / 'Report.xlsx'
    if dst.exists():
        raise FileExistsError(dst)

    logger.info('src="{}"', src)
    logger.info('dst="{}"', dst)

    _read_reports(paths).write_excel(dst, column_widths=150)


if __name__ == '__main__':
    app.meta()
