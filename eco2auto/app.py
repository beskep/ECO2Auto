from collections.abc import Iterable
from pathlib import Path
from typing import Annotated

import polars as pl
from cyclopts import App, Group, Parameter
from loguru import logger

from eco2auto.automate import BatchRunner, Overwrite
from eco2auto.report import Eco2GraphReport
from eco2auto.utils import set_logger


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


app = App(help_format='markdown')
app.meta.group_parameters = Group('Option', sort_key=0)


@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    loglevel: Annotated[str | int, Parameter(name=['--loglevel', '-l'])] = 20,
):
    set_logger(level=loglevel)
    app(tokens)


Source = Annotated[Path, Parameter(name=['SOURCE', '--src'])]
Destination = Annotated[Path | None, Parameter(name=['DESTINATION', '--dst'])]
Extension = Annotated[tuple[str, ...], Parameter(name=['--extension', '-e'])]
_Overwrite = Annotated[Overwrite, Parameter(name=['--overwrite', '-o'])]
Restart = Annotated[int, Parameter(name=['--restart', '-r'])]


@app.command
def run(  # noqa: PLR0913
    source: Source,
    destination: Destination = None,
    *,
    extension: Extension = ('eco', 'ecox', 'tpl', 'tplx'),
    overwrite: _Overwrite = 'skip',
    restart: Restart = 0,
    recursive: bool = True,
):
    """
    ECO2 자동 평가 및 결과 저장.

    Parameters
    ----------
    source : Path
        대상 경로. ECO2 파일 또는 ECO2 파일이 저장된 폴더 경로.
    destination : Path | None, optional
        결과 저장 경로. 미지정 시 `source`와 같은 폴더.
    extension : Extension, optional
        대상 ECO2 파일 확장자.
    overwrite : Overwrite, optional
        결과 파일이 이미 존재하는 경우
        오류 발생 (`raise`), 덮어쓰기 (`overwrite`), 또는 넘기기 (`skip`).
    restart : int, optional
        0이 아닌 경우 `restart`회마다 ECO2를 재시작.
    recursive : bool, optional
        `source` 경로에서 ECO2 파일 재귀적 탐색 여부.
    """
    ext = tuple(x if x.startswith('.') else f'.{x}' for x in extension)
    runner = BatchRunner(
        src=source,
        dst=destination,
        extension=ext,
        overwrite=overwrite,
        restart=restart,
        recursive=recursive,
    )
    runner.run()


@app.command
def report(source: Source, destination: Destination = None):
    """
    다수의 ECO2 결과 파일을 읽고 엑셀 파일로 저장.

    Parameters
    ----------
    source : Source
        대상 폴더. `source`에 위치한 `.xls` 파일을 전부 읽음.
    destination : Destination, optional
        저장 경로. 미지정 시 `source` 폴더 아래 `Report.xlsx` 파일 저장.

    Raises
    ------
    NotADirectoryError
        `source`가 directory가 아닐 때.
    FileNotFoundError
        `source` 아래 `.xls` 파일이 없을 때.
    FileExistsError
        저장 경로와 같은 파일이 이미 존재할 때.
    """
    if not source.is_dir():
        raise NotADirectoryError(source)
    if not (paths := list(source.glob('*.xls'))):
        msg = f'다음 경로에서 결과 파일을 찾지 못했습니다: {source}'
        raise FileNotFoundError(msg)

    destination = destination or source / 'Report.xlsx'
    if destination.exists():
        raise FileExistsError(destination)

    logger.info('src="{}"', source)
    logger.info('dst="{}"', destination)

    _read_reports(paths).write_excel(destination, column_widths=150)


if __name__ == '__main__':
    app.meta()
