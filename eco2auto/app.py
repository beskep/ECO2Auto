from collections.abc import Container, Iterable
from pathlib import Path
from typing import Annotated

import polars as pl
from cyclopts import App, Group, Parameter
from loguru import logger

from eco2auto.automate import Eco2App, Overwrite
from eco2auto.report import Eco2GraphReport
from eco2auto.utils import Progress, set_logger


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


def _source(
    path: Path,
    suffix: Container[str] = ('.eco', '.ecox', '.tpl', '.tplx'),
):
    if not path.is_dir():
        yield path
        return

    for p in path.glob('*'):
        if p.is_file() and p.suffix.lower() in suffix:
            yield p


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


@app.command
def run(
    source: Source,
    destination: Destination = None,
    *,
    extension: Extension = ('eco', 'ecox', 'tpl', 'tplx'),
    overwrite: _Overwrite = 'raise',
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
    """
    extension = tuple(x if x.startswith('.') else f'.{x}' for x in extension)

    if not (src := list(_source(source, suffix=extension))):
        logger.warning('ECO2 파일을 찾을 수 없습니다. 프로그램을 종료합니다.')
        return

    eco = Eco2App(overwrite=overwrite)
    eco.check_dst(src=src, dst=destination)

    with Progress() as p:
        paths = list(eco.batch_run(src=p.track(src), dst=destination))

    eco.close()

    if paths:
        path = paths[0].dst.parent / 'Report.xlsx'
        logger.info('결과 파일 경로: "{}"', path)

        if path.exists() and overwrite != 'overwrite':
            logger.warning('결과 파일이 이미 존재합니다. 결과를 저장하지 않습니다.')
            return

        _read_reports(p.dst for p in paths).write_excel(path)


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
