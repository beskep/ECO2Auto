from __future__ import annotations

import os
from collections.abc import Iterable
from contextlib import suppress
from itertools import repeat
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple

from loguru import logger
from more_itertools import always_iterable
from pywinauto import ElementNotFoundError, keyboard
from pywinauto.application import Application, WindowSpecification

if TYPE_CHECKING:
    from _typeshed import StrPath

    IterPath = StrPath | Iterable[StrPath]

Overwrite = Literal['raise', 'overwrite', 'skip']


class NotAbsolutePathError(OSError):
    pass


def find_eco2():
    p = 'ECO2_*/Eco2Ar.exe'
    if not (paths := list(Path('C:/').glob(p))):
        raise FileNotFoundError(p)

    if len(paths) > 1:
        logger.info('ECO2 경로를 2개 이상 발견함: {}', paths)

    return paths[-1]


class Eco2Path(NamedTuple):
    src: Path
    dst: Path
    root: Path | None

    def relative(self, min_root_len: int = 10):
        if self.root is None or (len(self.root.as_posix()) < min_root_len):
            return type(self)(self.src, self.dst, None)

        return type(self)(
            self.src.relative_to(self.root),
            self.dst.relative_to(self.root),
            self.root,
        )

    @classmethod
    def create(cls, src: StrPath, dst: StrPath | None):
        s = Path(src)

        if dst is None:
            r = s.parent
            d = s.with_suffix('.xls')
        else:
            if (d := Path(dst)).is_dir():
                d /= f'{s.stem}.xls'

            r = Path(os.path.commonpath([str(s), str(d)]))

        return cls(s, d, r)

    @classmethod
    def iter(
        cls,
        src: IterPath,
        dst: IterPath | None,
    ):
        its: Iterable[StrPath] = always_iterable(src)

        itd: Iterable[StrPath | None]
        if dst is None or (not isinstance(dst, Iterable) and Path(dst).is_dir()):
            itd = repeat(dst)
            strict = False
        else:
            itd = always_iterable(dst)
            strict = True

        for s, d in zip(its, itd, strict=strict):
            yield cls.create(src=s, dst=d)


class Eco2App:
    TITLE_RE = '건물에너지평가프로그램.*'

    def __init__(
        self,
        *,
        connect=True,
        overwrite: Overwrite = 'raise',
    ) -> None:
        app = Application(backend='uia')

        if connect:
            with suppress(ElementNotFoundError):
                app = app.connect(title_re=self.TITLE_RE)
                logger.trace('connect "{}"', self.TITLE_RE)

        if not app.is_process_running():
            path = find_eco2()
            logger.trace('start "{}"', path)
            app = app.start(str(path))

        window = app.window(title_re=self.TITLE_RE)

        # 새로 연 프로세스의 경우 LOGIN
        window.set_focus()
        if (login := window.child_window(title='사용자확인')).exists():
            login.child_window(title='LOGIN', depth=1).click_input()
            logger.trace('로그인')

        self.app: Application = app
        self.win: WindowSpecification = window
        self.overwrite: Overwrite = overwrite

    def open(self, path: StrPath):
        path = Path(path)
        if not path.is_absolute():
            raise NotAbsolutePathError(path)
        if not path.exists():
            raise FileNotFoundError(path)

        self.win.set_focus()
        keyboard.send_keys('^o')  # ctrl+o 파일열기

        # 경록 입력, 열기
        browser = self.win.child_window(title='열기', control_type='Window')
        (
            browser.child_window(title='파일 이름(N):', control_type='ComboBox')
            .child_window(title='파일 이름(N):', control_type='Edit')
            .set_edit_text(str(path))
        )
        browser.child_window(title='열기(O)').click_input()

        with suppress(ElementNotFoundError):
            (
                self.win.child_window(title='확인', depth=1)
                .child_window(title='아니요(N)', control_type='Button', depth=1)
                .click_input()
            )
            logger.trace('"현재 열려있는 프로젝트를 저장하시겠습니까?" 아니오')

        with suppress(ElementNotFoundError):
            (
                self.win.child_window(title='버전확인', depth=1)
                .child_window(title='닫기', depth=2)
                .click_input()
            )
            logger.trace(
                '"해당 파일은 현재 프로그램과 동일한 버젼에서 '
                '생성된 파일이 아닙니다." 닫기'
            )

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
        keyboard.send_keys('{ENTER}')

    def write_report(self, path: StrPath):
        path = Path(path)
        if not path.is_absolute():
            raise NotAbsolutePathError(path)
        if self.overwrite == 'raise' and path.exists():
            raise FileExistsError(path)

        # 결과그래프 창
        graph = self.app.window(title='결과그래프', control_type='Window')
        if not graph.exists():
            logger.trace('결과그래프 창 열기')
            self.win.set_focus()
            self.win.child_window(
                title='계산결과그래프보기', control_type='Button'
            ).click_input()

        # 엑셀 저장 버튼
        graph.set_focus()
        export = graph.child_window(title_re='Export|내보내기', control_type='MenuItem')
        export.click_input()
        export.child_window(title='Excel', control_type='MenuItem').click_input()

        # 경로 입력, 저장
        browser = graph.child_window(title='다른 이름으로 저장', control_type='Window')
        (
            browser.child_window(title='파일 이름:', control_type='ComboBox')
            .child_window(title='파일 이름:', control_type='Edit')
            .set_edit_text(str(path))
        )
        browser.child_window(title='저장(S)', control_type='Button').click_input()

        # 덮어쓰기 처리
        overwrite = browser.child_window(
            title='다른 이름으로 저장 확인', control_type='Window'
        )
        if overwrite.child_window(
            title_re=r'.*(이미 있습니다.\s*바꾸시겠습니까\?).*',
            auto_id='ContentText',
            control_type='Text',
        ).exists():
            logger.debug('결과 파일 존재함: {}', self.overwrite)

            match self.overwrite:
                case 'raise':
                    raise FileExistsError(path)
                case 'overwrite':
                    title = '예(Y)'
                case 'skip':
                    title = '아니요(N)'

            overwrite.child_window(title=title, control_type='Button').click_input()

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
            logger.trace('"현재 열려있는 파일을 저장 후 종료하시겠습니까?" 아니오')
        elif dialog.child_window(title_re='.*(종료하시겠습니까).*').exists():
            dialog.child_window(title='확인', control_type='Button').click_input()
            logger.trace('"종료하시겠습니까?" 확인')

    def run(self, src: StrPath, dst: StrPath | None = None):
        src = Path(src).absolute()
        dst = (Path(dst) if dst else src.with_suffix('.xls')).absolute()

        if self.overwrite == 'skip' and dst.exists():
            logger.info(
                '결과 파일이 이미 존재합니다. 설정에 따라 평가를 실행하지 않습니다.'
            )
            return

        logger.trace('open')
        self.open(src)

        logger.trace('calculate')
        self.calculate()

        logger.trace('write report')
        self.write_report(dst)

    def check_dst(self, src: IterPath, dst: IterPath | None = None):
        if self.overwrite != 'raise':
            return

        for p in Eco2Path.iter(src=src, dst=dst):
            if p.dst.exists():
                raise FileExistsError(p.dst)

    def batch_run(self, src: IterPath, dst: IterPath | None = None):
        for p in Eco2Path.iter(src=src, dst=dst):
            r = p.relative()

            if r.root:
                logger.debug('root={}', r.root.as_posix())
            logger.info('src={}', r.src.as_posix())
            logger.info('dst={}', r.dst.as_posix())

            self.run(src=p.src, dst=p.dst)

            yield p
