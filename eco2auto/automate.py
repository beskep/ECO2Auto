from __future__ import annotations

import contextlib
import dataclasses as dc
import functools
from collections.abc import Sequence  # noqa: TC003
from pathlib import Path
from typing import Literal

from loguru import logger
from pywinauto import findwindows, keyboard, timings
from pywinauto.application import Application, WindowSpecification

from eco2auto.utils import Progress

Overwrite = Literal['raise', 'overwrite', 'skip']
Report = Literal[
    'graph',  # 결과그래프
    'calculations',  # 계산결과
    'certificate',  # 인증평가서
    'upload',  # 업로드 양식
]
REPORTS: dict[Report, str] = {
    'graph': '결과그래프',
    'calculations': '계산결과',
    'certificate': '인증평가서',
    'upload': '업로드양식',
}


class NotAbsolutePathError(OSError):
    pass


def find_eco2(p: str = 'ECO2_*/Eco2Ar.exe'):
    if not (paths := list(Path('C:/').glob(p))):
        raise FileNotFoundError(p)

    if len(paths) > 1:
        logger.info('ECO2 경로를 2개 이상 발견함: {}', paths)

    return paths[-1]


@dc.dataclass
class Case:
    model: Path  # eco, tpl
    output: Path | None
    reports: Sequence[Report] = ('graph', 'calculations')

    @dc.dataclass(frozen=True)
    class Paths:
        graph: Path | None
        calculations: Path | None

        @functools.cached_property
        def count(self):
            return sum(x is not None for x in dc.asdict(self).values())

        @functools.cached_property
        def exists(self):
            # cached_property 사용 시 BatchRunner에서 최초 확인 후
            # 파일 존재 여부가 달라질 수 있기 때문에 Eco2App에서 한번 더 확인 필요
            return sum(x is not None and x.exists() for x in dc.asdict(self).values())

    def __post_init__(self):
        if self.model.suffix.lower().startswith('.eco'):
            self.reports = tuple(x for x in self.reports if x != 'calculations')

    @functools.cached_property
    def paths(self):
        output = (self.output or self.model.parent).absolute()
        stem = self.model.stem

        def path(report: Report):
            if report not in self.reports:
                return None

            return output / f'{stem} {REPORTS[report]}.xls'

        return self.Paths(
            graph=path('graph'),
            calculations=path('calculations'),
        )


class Eco2App:
    TITLE_RE = '건물에너지평가프로그램.*'

    def __init__(self, *, connect=True, calculation_timeout: float = 300) -> None:
        app = Application(backend='uia')

        if connect:
            with contextlib.suppress(findwindows.ElementNotFoundError):
                app = app.connect(title_re=self.TITLE_RE)

        if not app.is_process_running():
            path = find_eco2()
            app = app.start(str(path))

        window = app.window(title_re=self.TITLE_RE)

        # 새로 연 프로세스의 경우 LOGIN
        window.set_focus()
        if (login := window.child_window(title='사용자확인')).exists():
            login.set_focus()
            login.child_window(title='LOGIN', depth=1).click_input()

        self.app: Application = app
        self.win: WindowSpecification = window
        self.calculation_timeout: float = calculation_timeout

    def run(
        self,
        case: Path | Case,
        output: Path | None = None,
        reports: Sequence[Report] = ('graph', 'calculations'),
        overwrite: Overwrite = 'skip',
    ):
        case = case if isinstance(case, Case) else Case(case, output, reports)

        self.open(case.model)
        self.calculate()
        self.write_report(case.paths, overwrite)

    def open(self, path: str | Path):
        path = Path(path)
        if not path.is_absolute():
            raise NotAbsolutePathError(path)
        if not path.exists():
            raise FileNotFoundError(path)

        self.win.set_focus()
        keyboard.send_keys('^o')  # ctrl+o 파일열기

        # 경로 입력, 열기
        browser = self.win.child_window(title='열기', control_type='Window')
        (
            browser.child_window(title='파일 이름(N):', control_type='ComboBox')
            .child_window(title='파일 이름(N):', control_type='Edit')
            .set_edit_text(str(path))
        )
        browser.child_window(title='열기(O)').click_input()

        with contextlib.suppress(findwindows.ElementNotFoundError):
            (
                self.win.child_window(title='확인', depth=1)
                .child_window(title='아니요(N)', control_type='Button', depth=1)
                .click_input()
            )

        with contextlib.suppress(findwindows.ElementNotFoundError):
            (
                self.win.child_window(title='버전확인', depth=1)
                .child_window(title='닫기', depth=2)
                .click_input()
            )

    @functools.cached_property
    def completion_window(self):
        return WindowSpecification({
            'backend': 'win32',
            'top_level_only': True,
            'title': '완료',
        })

    def calculate(self):
        self.close_graph()

        self.win.set_focus()
        self.win.child_window(title='계산', control_type='Button').click_input()
        (
            self.win.child_window(title='계산', control_type='Window')
            .child_window(title='요구량+소요량', control_type='Button')
            .click_input()
        )

        # "완료" 창
        self.completion_window.wait('exists', timeout=self.calculation_timeout)
        keyboard.send_keys('{ENTER}')

    def write_report(self, paths: Case.Paths, overwrite: Overwrite = 'skip'):
        exits = paths.exists
        if exits == paths.count:
            if overwrite == 'raise':
                raise FileExistsError(paths)
            if overwrite == 'skip':
                logger.info(
                    '결과 파일이 이미 존재합니다. 설정에 따라 평가를 실행하지 않습니다.'
                )
                return
        elif 0 < exits < paths.count:
            overwrite = 'overwrite'

        if paths.graph:
            self._write_report_graph(paths.graph, overwrite)
        if paths.calculations:
            self._write_report_calculations(paths.calculations, overwrite)

    def _write_report_graph(self, path: Path, overwrite: Overwrite = 'skip'):
        win = self.app.window(title='결과그래프', control_type='Window')
        if not win.exists():
            self.win.set_focus()
            self.win.child_window(
                title='계산결과그래프보기', control_type='Button'
            ).click_input()

        self._write_report(win, path, overwrite)

    def _write_report_calculations(self, path: Path, overwrite: Overwrite = 'skip'):
        win = self.app.window(title_re='계산결과.*', control_type='Window')
        if not win.exists():
            self.close_graph()
            self.win.set_focus()

            kwargs = {'title': '계산결과', 'control_type': 'MenuItem'}
            self.win.child_window(**kwargs).click_input()
            self.win.child_window(**kwargs, found_index=1).click_input()

        self._write_report(win, path, overwrite)
        win.close()

    @staticmethod
    def _write_report(
        win: WindowSpecification,
        path: Path,
        overwrite: Overwrite = 'skip',
    ):
        win.set_focus()

        # 엑셀 저장 버튼
        export = win.child_window(title_re='Export|내보내기', control_type='MenuItem')
        export.click_input()
        export.child_window(title='Excel', control_type='MenuItem').click_input()

        # 경로 입력, 저장
        browser = win.child_window(title='다른 이름으로 저장', control_type='Window')
        (
            browser.child_window(title='파일 이름:', control_type='ComboBox')
            .child_window(title='파일 이름:', control_type='Edit')
            .set_edit_text(str(path))
        )
        browser.child_window(title='저장(S)', control_type='Button').click_input()

        # 덮어쓰기 처리
        dialog = browser.child_window(
            title='다른 이름으로 저장 확인', control_type='Window'
        )
        if dialog.child_window(
            title_re=r'.*(이미 있습니다.\s*바꾸시겠습니까\?).*',
            auto_id='ContentText',
            control_type='Text',
        ).exists():
            logger.debug('결과 파일 존재함: {}', overwrite)

            match overwrite:
                case 'raise':
                    raise FileExistsError(path)
                case 'overwrite':
                    title = '예(Y)'
                case 'skip':
                    title = '아니요(N)'

            dialog.child_window(title=title, control_type='Button').click_input()

    def close_graph(self):
        graph = self.app.window(title='결과그래프', control_type='Window')
        if graph.exists():
            graph.set_focus()
            graph.close()

    def close(self):
        self.close_graph()

        self.win.set_focus()
        self.win.close()

        dialog = self.win.child_window(title='확인', control_type='Window')

        if dialog.child_window(title_re='.*(열려있는 파일을 저장).*').exists():
            dialog.child_window(title='아니요(N)', control_type='Button').click_input()
        elif dialog.child_window(title_re='.*(종료하시겠습니까).*').exists():
            dialog.child_window(title='확인', control_type='Button').click_input()


@dc.dataclass
class BatchRunner:
    src: Path
    dst: Path | None = None

    _: dc.KW_ONLY

    report: Sequence[Report] = ('graph', 'calculations')
    extension: Literal['eco', 'tpl', 'any'] = 'any'
    overwrite: Overwrite = 'skip'
    timeout: float = 300
    restart: int = 0  # restart every
    retry: int = 100  # restart on error
    recursive: bool = True

    def __post_init__(self):
        if not self.src.is_dir():
            raise NotADirectoryError(self.src)

        if isinstance(self.dst, Path) and not self.dst.is_dir():
            raise NotADirectoryError(self.dst)

    def cases(self):
        if self.extension == 'any':
            ext = {'.eco', '.ecox', '.tpl', '.tplx'}
        else:
            ext = {f'.{self.extension}', f'{self.extension}x'}

        glob = self.src.glob('**/*' if self.recursive else '*')
        models = tuple(x for x in glob if x.suffix in ext)
        return tuple(Case(x, self.dst, self.report) for x in models)

    def _run(self):
        cases = self.cases()

        if self.overwrite == 'raise' and any(x.paths.exists for x in cases):
            raise FileExistsError([x for x in cases if x.paths.exists])

        cases = tuple(sorted(cases, key=lambda x: (-x.paths.exists, x.model)))
        w = len(str(len(cases)))
        count = 0

        app = Eco2App(calculation_timeout=self.timeout)

        for case in Progress.iter(cases):
            if self.overwrite == 'skip' and case.paths.exists == case.paths.count:
                continue

            logger.info('#{} | case={}', f'{count:0{w}d}', case.model)
            app.run(case)

            if self.restart and count and (count % self.restart) == 0:
                logger.info('Restart ECO2')
                app.close()
                app = Eco2App()

            count += 1

        app.close()

    def run(self):
        for retry in range(self.retry):
            if retry:
                logger.info('retry #{}', retry + 1)

            try:
                self._run()
            except (
                findwindows.ElementAmbiguousError,
                findwindows.ElementNotFoundError,
                findwindows.WindowAmbiguousError,
                findwindows.WindowNotFoundError,
                timings.TimeoutError,
            ) as e:
                logger.warning(repr(e))
            else:
                break

            keyboard.send_keys('{ESC}')
