"""RewriteConfig: Pydantic model for source-to-destination path prefix rewriting."""
from pydantic import BaseModel, field_validator


class RewriteConfig(BaseModel):
    """Configuration for path prefix rewriting.

    Validates that both prefixes use s3:// or s3a:// schemes and strips
    trailing slashes so prefix comparisons are unambiguous.

    Example:
        cfg = RewriteConfig(
            src_prefix="s3a://minio-bucket/warehouse",
            dst_prefix="s3://aws-bucket/warehouse",
        )
        rewritten = cfg.replace_prefix("s3a://minio-bucket/warehouse/db/table")
        # "s3://aws-bucket/warehouse/db/table"
    """

    src_prefix: str  # e.g. "s3a://minio-bucket/warehouse"
    dst_prefix: str  # e.g. "s3://aws-bucket/warehouse"

    @field_validator("src_prefix", "dst_prefix")
    @classmethod
    def must_be_s3_uri(cls, v: str) -> str:
        """Validate prefix scheme and strip trailing slash."""
        if not v:
            raise ValueError("Prefix must not be empty")
        if not (v.startswith("s3://") or v.startswith("s3a://")):
            raise ValueError(f"Prefix must start with s3:// or s3a://: {v!r}")
        return v.rstrip("/")

    def replace_prefix(self, path: str) -> str:
        """Replace src_prefix with dst_prefix in path.

        Returns the path unchanged if it does not start with src_prefix.
        This handles s3a:// to s3:// normalization (PATH-06) when src_prefix
        starts with s3a:// and dst_prefix starts with s3://.

        Args:
            path: The path to rewrite (e.g. an S3 URI from Iceberg metadata).

        Returns:
            Rewritten path with src_prefix replaced by dst_prefix, or original
            path if it does not match.
        """
        if path.startswith(self.src_prefix):
            return self.dst_prefix + path[len(self.src_prefix):]
        return path
