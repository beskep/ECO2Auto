import dataclasses as dc
from itertools import chain
from pathlib import Path
from typing import IO

import polars as pl
import polars.selectors as cs


def _key_value(data: str):
    try:
        return (None, float(data))
    except ValueError:
        return (data.rstrip(' :'), None)


def as_float(expr: pl.Expr, *, strict: bool = False):
    return (
        expr.str.replace_all(',', '')
        .str.strip_suffix('%')
        .cast(pl.Float64, strict=strict)
    )


class EmptyDataError(ValueError):
    pass


@dc.dataclass
class Eco2ReportBase:
    source: str | Path | IO[bytes] | bytes

    def data(self, *args, **kwargs) -> pl.DataFrame:
        raise NotImplementedError


@dc.dataclass
class Eco2GraphRows:
    monthly: tuple[int, int] = (0, 2)
    yearly: tuple[int, int] = (2, 8)
    stats: int = -3


@dc.dataclass
class Eco2GraphReport(Eco2ReportBase):
    """`그래프 > 계산결과그래프` 파일."""

    rows: Eco2GraphRows = dc.field(default_factory=Eco2GraphRows)

    building_type: str = dc.field(init=False)
    monthly: pl.DataFrame = dc.field(init=False)
    yearly: pl.DataFrame = dc.field(init=False)
    misc: dict[str, float] = dc.field(init=False)

    def __post_init__(self):
        raw = pl.read_excel(self.source)

        if 'No Data' in raw:
            raise EmptyDataError

        self.building_type, self.monthly = self._monthly(raw)
        self.yearly = self._yearly(raw)
        self.misc = self._stats(raw)

    def _stats(self, raw: pl.DataFrame):
        tail = raw[self.rows.stats :]
        stats = tail.select([
            s.name
            for s in tail
            if not s.is_null().all() and '％' not in s  # noqa: RUF001
        ])

        kv = [_key_value(x) for x in chain.from_iterable(stats.iter_rows()) if x]
        keys = (k for k, _ in kv if k is not None)
        values = (v for _, v in kv if v is not None)

        return dict(zip(keys, values, strict=True))

    def _monthly(self, raw: pl.DataFrame):
        r = self.rows.monthly
        df = raw.drop(cs.contains('UNNAMED'))[r[0] : r[1]]

        if (width := df.width) != 13:  # noqa: PLR2004
            msg = f'{width=} != 13'
            raise AssertionError(msg)

        building_type = df.columns[0]
        monthly = (
            df.rename({building_type: 'energy'})
            .with_columns(pl.all().exclude('energy').cast(pl.Float64))
            .unpivot(index='energy', variable_name='month')
            .with_columns(
                pl.col('month').str.strip_suffix('월').cast(pl.Int8),
                pl.lit('kWh/m²').alias('unit'),
            )
            .sort('energy', 'month')
        )

        return building_type, monthly

    def _yearly(self, raw: pl.DataFrame):
        r = self.rows.yearly
        df = raw[r[0] + 1 : r[1]]

        null = [s.name for s in df if s.is_null().all()]
        columns = raw.drop(null).row(r[0])

        df = df.drop(null)
        df.columns = ['variable', *columns[1:]]

        return (
            df.with_columns(pl.all().exclude('variable').cast(pl.Float64))
            .unpivot(index='variable', variable_name='energy')
            .with_columns(
                pl.col('variable')
                .replace_strict('CO2발생량', 'kgCO₂/m²', default='kWh/m²yr')
                .alias('unit')
            )
            .sort('variable', 'energy')
        )

    def _misc(self, *, upload_format: bool = False):
        df = (
            pl.DataFrame(
                list(self.misc.items()),
                schema=[('variable', pl.String), ('value', pl.Float64)],
                orient='row',
            )
            .select(
                pl.lit('기타').alias('category'),
                pl.all(),
                pl.col('variable')
                .str.extract('^((단위면적당)|(에너지자립률)).*')
                .replace_strict({'단위면적당': 'kWh/m²yr', '에너지자립률': '%'})
                .alias('unit'),
            )
            .sort(pl.col('variable'))
        )

        if upload_format:
            bldg = pl.DataFrame({'category': '건물개요', 'value': self.building_type})
            zeb = df.with_columns(pl.lit('제로에너지건축물').alias('category'))
            df = pl.concat([bldg, zeb], how='diagonal_relaxed')

        return df

    def data(self, *, upload_format: bool = False):
        monthly = self.monthly.select(
            pl.lit('월별').alias('category'),
            pl.lit('요구량').alias('variable'),
            pl.all(),
        )
        yearly = self.yearly.select(pl.lit('연간').alias('category'), pl.all())
        misc = self._misc(upload_format=upload_format)

        df = pl.concat([monthly, yearly, misc], how='diagonal_relaxed')

        if upload_format:
            df = df.with_columns(
                pl.col('category').replace({
                    '월별': '평가결과(월별)',
                    '연간': '평가결과',
                }),
                pl.col('variable').replace({
                    '요구량': '에너지요구량',
                    '소요량': '에너지소요량',
                    '1차소요량': '1차에너지소요량',
                    '등급용1차소요량': '등급산출용 1차에너지소요량',
                    'CO2발생량': '단위면적당CO₂발생량',
                }),
                pl.col('energy').str.strip_suffix('에너지'),
                pl.format('{}월', pl.col('month')),
            ).rename({
                'category': '구분',
                'variable': '항목(대)',
                'energy': '항목(중)',
                'month': '항목(소)',
                'value': '값',
                'unit': '단위',
            })

        return df


@dc.dataclass
class Eco2UploadReport(Eco2ReportBase):
    """`계산결과 > 업로드양식` 파일."""

    raw: pl.DataFrame = dc.field(init=False)

    def __post_init__(self):
        self.raw = pl.read_excel(self.source).with_columns(
            pl.col('단위')
            .replace({'-': None})
            .str.replace_many(['㎡', '년', '•'], ['m²', 'yr', ''])
        )

    def data(self, *, numeric: bool = False, drop_code: bool = False):
        df = self.raw

        if drop_code:
            df = df.drop(cs.ends_with('코드'))

        if numeric:
            df = df.with_columns(
                pl.when(pl.col('값').str.ends_with('%'))
                .then(pl.lit('%'))
                .otherwise(pl.col('단위'))
                .alias('단위'),
                as_float(pl.col('값')),
            )

        return df
