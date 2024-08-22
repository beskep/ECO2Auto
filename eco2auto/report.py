import dataclasses as dc
from functools import cached_property
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


class EmptyDataError(ValueError):
    pass


@dc.dataclass
class Eco2ReportRows:
    monthly: tuple[int, int] = (0, 2)
    yearly: tuple[int, int] = (2, 8)
    stats: int = -3


@dc.dataclass
class Eco2Report:
    source: str | Path | IO[bytes] | bytes
    rows: Eco2ReportRows = dc.field(default_factory=Eco2ReportRows)

    building_type: str = dc.field(init=False)
    monthly: pl.DataFrame = dc.field(init=False)
    yearly: pl.DataFrame = dc.field(init=False)
    stats: dict[str, float] = dc.field(init=False)

    def __post_init__(self):
        raw = pl.read_excel(self.source)

        if 'No Data' in raw:
            raise EmptyDataError

        self.stats = self._stats(raw)
        self.building_type, self.monthly = self._monthly(raw)
        self.yearly = self._yearly(raw)

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
            .with_columns(pl.lit('kWh/m²yr').alias('unit'))
            .sort('variable', 'energy')
        )

    @cached_property
    def dataframe(self) -> pl.DataFrame:
        monthly = self.monthly.select(
            pl.lit('월별').alias('category'),
            pl.lit('요구량').alias('variable'),
            pl.all(),
        )
        yearly = self.yearly.select(pl.lit('연간').alias('category'), pl.all())
        stats = (
            pl.DataFrame(
                list(self.stats.items()),
                schema=[('variable', pl.String), ('value', pl.Float64)],
                orient='row',
            )
            .sort('variable')
            .select(
                pl.lit('기타').alias('category'),
                pl.all(),
                pl.when(pl.col('variable').str.starts_with('단위면적당'))
                .then(pl.lit('kWh/m²yr'))
                .when(pl.col('variable').str.starts_with('에너지자립률'))
                .then(pl.lit('%'))
                .otherwise(pl.lit(None))
                .alias('unit'),
            )
        )

        return pl.concat([monthly, yearly, stats], how='diagonal')
