"""
S3 데이터 복사 모듈
boto3 기반 S3 간 데이터 복사
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

console = Console()


@dataclass
class CopyResult:
    """복사 결과"""

    source_path: str
    target_path: str
    files_copied: int
    bytes_copied: int
    status: str  # success, error, skipped
    error: str | None = None


class S3Copier:
    """S3 데이터 복사기"""

    def __init__(
        self,
        aws_profile: str = "default",
        aws_region: str = "ap-northeast-2",
        source_endpoint_url: str | None = None,
        target_endpoint_url: str | None = None,
        max_workers: int = 10,
    ):
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        # 빈 문자열은 None으로 변환
        self.source_endpoint_url = source_endpoint_url or None
        self.target_endpoint_url = target_endpoint_url or None
        self.max_workers = max_workers

        # boto3 클라이언트
        # profile이 지정되어 있으면 사용 (default 포함)
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
        else:
            # 비어있으면 profile 없이 세션 생성 (환경변수/IAM 역할 사용)
            session = boto3.Session(region_name=aws_region)

        config = Config(
            max_pool_connections=max_workers * 2,
            retries={"max_attempts": 3},
        )

        self.source_s3 = session.client(
            "s3",
            endpoint_url=self.source_endpoint_url,
            config=config,
        )
        self.target_s3 = session.client(
            "s3",
            endpoint_url=self.target_endpoint_url,
            config=config,
        )

    def list_objects(
        self,
        bucket: str,
        prefix: str,
        use_source: bool = True,
    ) -> list[dict[str, Any]]:
        """S3 객체 목록 조회"""
        s3 = self.source_s3 if use_source else self.target_s3
        objects = []

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append(
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"],
                    }
                )

        return objects

    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        target_bucket: str,
        target_key: str,
    ) -> tuple[bool, str | None]:
        """단일 객체 복사. (성공여부, 에러메시지) 반환"""
        try:
            # 소스에서 객체 다운로드
            response = self.source_s3.get_object(Bucket=source_bucket, Key=source_key)
            body = response["Body"].read()

            # 타겟에 업로드
            a = self.target_s3.put_object(
                Bucket=target_bucket,
                Key=target_key,
                Body=body,
                ContentType=response.get("ContentType", "application/octet-stream"),
            )
            return True, None
        except Exception as e:
            return False, str(e)

    def copy_prefix(
        self,
        source_bucket: str,
        source_prefix: str,
        target_bucket: str,
        target_prefix: str,
        dry_run: bool = False,
        silent: bool = False,
    ) -> CopyResult:
        """prefix 단위 복사 (동기)"""
        # 소스 객체 목록 조회
        objects = self.list_objects(source_bucket, source_prefix, use_source=True)

        if not objects:
            return CopyResult(
                source_path=f"s3://{source_bucket}/{source_prefix}",
                target_path=f"s3://{target_bucket}/{target_prefix}",
                files_copied=0,
                bytes_copied=0,
                status="skipped",
                error="No objects found",
            )

        if dry_run:
            total_size = sum(obj["size"] for obj in objects)
            if not silent:
                console.print(
                    f"  [yellow][DRY-RUN] {len(objects)}개 파일, {total_size / 1024 / 1024:.2f} MB[/yellow]"
                )
            return CopyResult(
                source_path=f"s3://{source_bucket}/{source_prefix}",
                target_path=f"s3://{target_bucket}/{target_prefix}",
                files_copied=len(objects),
                bytes_copied=total_size,
                status="dry_run",
            )

        files_copied = 0
        bytes_copied = 0
        errors = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for obj in objects:
                source_key = obj["key"]
                # prefix 치환
                relative_key = source_key[len(source_prefix) :].lstrip("/")
                target_key = f"{target_prefix}/{relative_key}".lstrip("/")

                future = executor.submit(
                    self.copy_object,
                    source_bucket,
                    source_key,
                    target_bucket,
                    target_key,
                )
                futures.append((future, obj["size"], source_key))

            for future, size, key in futures:
                success, error = future.result()
                if success:
                    files_copied += 1
                    bytes_copied += size
                else:
                    errors.append(f"{key}: {error}")

        if errors:
            error_msg = f"{len(errors)}개 복사 실패: {errors[0]}"
            return CopyResult(
                source_path=f"s3://{source_bucket}/{source_prefix}",
                target_path=f"s3://{target_bucket}/{target_prefix}",
                files_copied=files_copied,
                bytes_copied=bytes_copied,
                status="error",
                error=error_msg,
            )

        return CopyResult(
            source_path=f"s3://{source_bucket}/{source_prefix}",
            target_path=f"s3://{target_bucket}/{target_prefix}",
            files_copied=files_copied,
            bytes_copied=bytes_copied,
            status="success",
        )

    async def copy_prefix_async(
        self,
        source_bucket: str,
        source_prefix: str,
        target_bucket: str,
        target_prefix: str,
        dry_run: bool = False,
    ) -> CopyResult:
        """prefix 단위 복사 (비동기 래퍼)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.copy_prefix,
            source_bucket,
            source_prefix,
            target_bucket,
            target_prefix,
            dry_run,
        )

    def copy_partitions(
        self,
        source_bucket: str,
        source_base_prefix: str,
        target_bucket: str,
        target_base_prefix: str,
        partitions: list[dict[str, Any]],
        partition_columns: list[str],
        max_parallel: int = 5,
        dry_run: bool = False,
        silent: bool = False,
    ) -> list[CopyResult]:
        """파티션 단위 복사"""
        results = []

        if silent:
            # Silent 모드: Progress 없이 실행
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                futures = []

                for partition in partitions:
                    partition_path = "/".join(
                        f"{col}={partition.get(col, '')}" for col in partition_columns
                    )

                    source_prefix = f"{source_base_prefix}/{partition_path}".lstrip("/")
                    target_prefix = f"{target_base_prefix}/{partition_path}".lstrip("/")

                    future = executor.submit(
                        self.copy_prefix,
                        source_bucket,
                        source_prefix,
                        target_bucket,
                        target_prefix,
                        dry_run,
                    )
                    futures.append(future)

                for future in futures:
                    result = future.result()
                    results.append(result)
        else:
            # Normal 모드: Progress bar 표시
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=console,
            )

            with progress:
                task = progress.add_task("파티션 복사", total=len(partitions))

                with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                    futures = []

                    for partition in partitions:
                        partition_path = "/".join(
                            f"{col}={partition.get(col, '')}"
                            for col in partition_columns
                        )

                        source_prefix = f"{source_base_prefix}/{partition_path}".lstrip(
                            "/"
                        )
                        target_prefix = f"{target_base_prefix}/{partition_path}".lstrip(
                            "/"
                        )

                        future = executor.submit(
                            self.copy_prefix,
                            source_bucket,
                            source_prefix,
                            target_bucket,
                            target_prefix,
                            dry_run,
                        )
                        futures.append(future)

                    for future in futures:
                        result = future.result()
                        results.append(result)
                        progress.advance(task)

        return results

    def ensure_bucket_exists(
        self, bucket: str, use_target: bool = True, silent: bool = False
    ):
        """버킷 존재 확인 및 생성"""
        s3 = self.target_s3 if use_target else self.source_s3
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            if not silent:
                console.print(f"  [yellow]버킷 생성: {bucket}[/yellow]")
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": self.aws_region},
            )

    def get_prefix_size(
        self,
        bucket: str,
        prefix: str,
        use_source: bool = True,
    ) -> tuple[int, int]:
        """prefix의 총 파일 수와 크기 조회"""
        objects = self.list_objects(bucket, prefix, use_source)
        total_files = len(objects)
        total_size = sum(obj["size"] for obj in objects)
        return total_files, total_size

    def delete_prefix(
        self,
        bucket: str,
        prefix: str,
        use_target: bool = True,
    ) -> int:
        """prefix 아래 모든 객체 삭제"""
        s3 = self.target_s3 if use_target else self.source_s3
        objects = self.list_objects(bucket, prefix, use_source=not use_target)

        if not objects:
            return 0

        # 1000개씩 배치 삭제 (S3 API 제한)
        deleted = 0
        for i in range(0, len(objects), 1000):
            batch = objects[i : i + 1000]
            delete_objects = [{"Key": obj["key"]} for obj in batch]
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": delete_objects},
            )
            deleted += len(batch)

        return deleted

    def verify_copy(
        self,
        source_bucket: str,
        source_prefix: str,
        target_bucket: str,
        target_prefix: str,
    ) -> dict[str, Any]:
        """소스와 타겟 S3 간 복사 결과 검증

        Returns:
            {
                "source_files": int,
                "target_files": int,
                "source_size": int,
                "target_size": int,
                "missing_in_target": list[str],
                "extra_in_target": list[str],
                "size_mismatch": list[dict],
                "status": "ok" | "mismatch" | "empty"
            }
        """
        source_objects = self.list_objects(
            source_bucket, source_prefix, use_source=True
        )
        target_objects = self.list_objects(
            target_bucket, target_prefix, use_source=False
        )

        # 소스 키를 상대 경로로 변환
        source_relative = {}
        for obj in source_objects:
            rel_key = obj["key"][len(source_prefix) :].lstrip("/")
            source_relative[rel_key] = obj

        # 타겟 키를 상대 경로로 변환
        target_relative = {}
        for obj in target_objects:
            rel_key = obj["key"][len(target_prefix) :].lstrip("/")
            target_relative[rel_key] = obj

        source_keys = set(source_relative.keys())
        target_keys = set(target_relative.keys())

        # 비교
        missing_in_target = list(source_keys - target_keys)
        extra_in_target = list(target_keys - source_keys)

        # 크기 불일치 검사
        size_mismatch = []
        for key in source_keys & target_keys:
            src_size = source_relative[key]["size"]
            tgt_size = target_relative[key]["size"]
            if src_size != tgt_size:
                size_mismatch.append(
                    {
                        "key": key,
                        "source_size": src_size,
                        "target_size": tgt_size,
                    }
                )

        # 상태 결정
        if not source_objects and not target_objects:
            status = "empty"
        elif not missing_in_target and not extra_in_target and not size_mismatch:
            status = "ok"
        else:
            status = "mismatch"

        return {
            "source_files": len(source_objects),
            "target_files": len(target_objects),
            "source_size": sum(obj["size"] for obj in source_objects),
            "target_size": sum(obj["size"] for obj in target_objects),
            "missing_in_target": missing_in_target,
            "extra_in_target": extra_in_target,
            "size_mismatch": size_mismatch,
            "status": status,
        }

    def debug_list(
        self,
        bucket: str,
        prefix: str,
        use_source: bool = True,
        show_details: bool = False,
    ) -> None:
        """S3 객체 목록 디버그 출력"""
        endpoint = self.source_endpoint_url if use_source else self.target_endpoint_url
        location = "소스" if use_source else "타겟"

        console.print(f"\n[bold cyan]S3 {location} 목록:[/bold cyan]")
        console.print(f"  버킷: {bucket}")
        console.print(f"  prefix: {prefix}")
        console.print(f"  endpoint: {endpoint or 'AWS 기본'}")

        try:
            objects = self.list_objects(bucket, prefix, use_source=use_source)

            if not objects:
                console.print(f"  [yellow]객체 없음[/yellow]")
                return

            console.print(f"  파일 수: {len(objects)}")
            total_size = sum(obj["size"] for obj in objects)
            console.print(f"  총 크기: {total_size / 1024 / 1024:.2f} MB")

            if show_details:
                console.print(f"\n  [dim]파일 목록:[/dim]")
                for obj in objects[:20]:  # 최대 20개
                    size_kb = obj["size"] / 1024
                    console.print(f"    {obj['key']} ({size_kb:.1f} KB)")
                if len(objects) > 20:
                    console.print(f"    [dim]... 외 {len(objects) - 20}개[/dim]")

        except Exception as e:
            console.print(f"  [red]에러: {e}[/red]")
