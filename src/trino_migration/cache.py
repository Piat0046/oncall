"""
데이터 캐싱 유틸리티
소스 Trino에서 추출한 데이터를 로컬 Parquet 파일로 저장/로드
"""

import json
import os
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

console = Console()


@dataclass
class CacheMetadata:
    """캐시 메타데이터"""
    source_catalog: str
    source_schema: str
    source_table: str
    columns: list[dict[str, str]]  # [{"name": "col1", "type": "VARCHAR"}, ...]
    row_count: int
    file_format: str = "PARQUET"
    ddl: str | None = None


class DataCache:
    """데이터 캐시 관리자"""

    def __init__(self, cache_dir: str = "./cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> Path:
        """캐시 경로 생성 (소스 기준)"""
        # 소스 테이블 기준으로 캐시 경로 생성
        safe_name = f"{catalog}.{schema}.{table}".replace("/", "_")
        return self.cache_dir / safe_name

    def _get_data_file(self, cache_path: Path) -> Path:
        return cache_path / "data.parquet"

    def _get_metadata_file(self, cache_path: Path) -> Path:
        return cache_path / "metadata.json"

    def exists(self, catalog: str, schema: str, table: str) -> bool:
        """캐시 존재 여부 확인"""
        cache_path = self._get_cache_path(catalog, schema, table)
        data_file = self._get_data_file(cache_path)
        metadata_file = self._get_metadata_file(cache_path)
        return data_file.exists() and metadata_file.exists()

    def save(
        self,
        catalog: str,
        schema: str,
        table: str,
        data: list[dict[str, Any]],
        columns: list[dict[str, str]],
        ddl: str | None = None,
    ) -> Path:
        """데이터를 Parquet 파일로 캐싱"""
        cache_path = self._get_cache_path(catalog, schema, table)
        cache_path.mkdir(parents=True, exist_ok=True)

        data_file = self._get_data_file(cache_path)
        metadata_file = self._get_metadata_file(cache_path)

        # 데이터를 PyArrow Table로 변환
        if data:
            # 컬럼명은 데이터에서 직접 추출 (메타데이터와 불일치 방지)
            col_names = list(data[0].keys()) if data else []
            arrays = {}
            for col_name in col_names:
                values = []
                for row in data:
                    val = row.get(col_name)
                    # 복잡한 타입을 문자열로 변환
                    if val is None:
                        values.append(None)
                    elif isinstance(val, (dict, list)):
                        values.append(json.dumps(val, ensure_ascii=False))
                    elif isinstance(val, (int, float, str, bool)):
                        values.append(val)
                    else:
                        # datetime, Decimal 등 PyArrow가 지원하는 타입은 그대로 유지
                        values.append(val)
                arrays[col_name] = values

            table_data = pa.table(arrays)
            pq.write_table(table_data, data_file)
            console.print(f"  [dim]캐시 저장: {data_file} ({len(data):,}건)[/dim]")
        else:
            # 빈 테이블도 저장
            pq.write_table(pa.table({}), data_file)
            console.print(f"  [dim]캐시 저장: {data_file} (0건)[/dim]")

        # 메타데이터 저장
        metadata = CacheMetadata(
            source_catalog=catalog,
            source_schema=schema,
            source_table=table,
            columns=columns,
            row_count=len(data),
            ddl=ddl,
        )
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(asdict(metadata), f, ensure_ascii=False, indent=2)

        return cache_path

    def load(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> tuple[list[dict[str, Any]], CacheMetadata]:
        """캐시에서 데이터 로드"""
        cache_path = self._get_cache_path(catalog, schema, table)
        data_file = self._get_data_file(cache_path)
        metadata_file = self._get_metadata_file(cache_path)

        if not data_file.exists() or not metadata_file.exists():
            raise FileNotFoundError(f"캐시를 찾을 수 없음: {cache_path}")

        # 메타데이터 로드
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata_dict = json.load(f)
        metadata = CacheMetadata(**metadata_dict)

        # 데이터 로드
        table_data = pq.read_table(data_file)
        data = table_data.to_pylist()

        console.print(f"  [dim]캐시 로드: {data_file} ({len(data):,}건)[/dim]")
        return data, metadata

    def delete(self, catalog: str, schema: str, table: str) -> bool:
        """캐시 삭제"""
        cache_path = self._get_cache_path(catalog, schema, table)
        if cache_path.exists():
            shutil.rmtree(cache_path)
            console.print(f"  [dim]캐시 삭제: {cache_path}[/dim]")
            return True
        return False

    def clear_all(self) -> int:
        """모든 캐시 삭제"""
        count = 0
        for item in self.cache_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
                count += 1
        console.print(f"[yellow]캐시 전체 삭제: {count}개[/yellow]")
        return count

    def list_cached(self) -> list[str]:
        """캐시된 테이블 목록"""
        cached = []
        for item in self.cache_dir.iterdir():
            if item.is_dir() and (item / "metadata.json").exists():
                cached.append(item.name)
        return cached
