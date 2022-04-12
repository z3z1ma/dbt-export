import os
from typing import List, Tuple
from collections import namedtuple
from pathlib import Path
import subprocess

# dbt stuff
import dbt.adapters.factory
import dbt.events.functions
from dbt.main import adapter_management
from dbt.adapters.factory import get_adapter
from dbt.tracking import disable_tracking
from dbt.flags import set_from_args
from dbt.config.runtime import RuntimeConfig
from dbt.parser.manifest import ManifestLoader

# numpy and pyarrow
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

disable_tracking()


def parquet_append(table, filepath: Path) -> None:
    table_existing = pq.read_table(source=filepath,  pre_buffer=False, use_threads=True, memory_map=True)  # Use memory map for speed.
    table = table.cast(table_existing.schema)
    # Make the handle atomic...
    handle = pq.ParquetWriter(filepath, table_existing.schema)  
    handle.write_table(table_existing)
    handle.write_table(table)
    handle.close()  # Writes binary footer.


def write_batch(rows: List[Tuple], columns: List[str], ref_parts: List[str]):
    ndarray = np.array(rows)
    ndarray_table = pa.table(
        {
            column_name: ndarray[:, column_index]
            for column_index, column_name in enumerate(columns)
        }
    )
    target_path = Path(OUTPUT_DIR, *ref_parts[:-1])
    target_path.mkdir(parents=True, exist_ok=True)  # Make Path
    target_file = target_path / f"{ref_parts[-1]}.parquet"
    if target_file.exists():
        parquet_append(ndarray_table, target_file)
    else:
        pq.write_table(ndarray_table, target_file)


RuntimeArgs = namedtuple(
    "RuntimeArgs", "project_dir profiles_dir single_threaded profile_name"
)

if __name__ == "__main__":
    BATCH_SIZE = 1e6
    OUTPUT_DIR = "output/"

    profile = "dev"
    project_dir = "./"

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = os.path.expanduser("~/.dbt")

    config = RuntimeConfig.from_args(
        RuntimeArgs(project_dir, profiles_dir, True, profile)
    )

    set_from_args("", config)

    # dbt.adapters.factory.reset_adapters()
    dbt.adapters.factory.register_adapter(config)

    adapter = get_adapter(config)

    # This loads manifest
    # models = subprocess.call(["dbt", "ls"]) ? Support selectors in our CLI by leveraging ls output
    manifest = ManifestLoader.get_full_manifest(config)

    # We should iter manifest + ls here
    for node, metadata in manifest.nodes.items():
        if not metadata.resource_type == "model":
            continue

        # Replace with dbt.adapter.base.BaseRelation.create_from_node(metadata)
        # or the equivalent
        ref_parts = []
        if metadata.schema:
            ref_parts.append(metadata.schema)
        if metadata.identifier:
            ref_parts.append(metadata.identifier)
        ref = ".".join(map(adapter.quote, ref_parts))


        stmt = f"SELECT * FROM {ref}"
        with adapter_management():
            with adapter.connection_named("dbt-export"):
                column_names: List[str] = []
                # Handle unique adapter implementations first
                if adapter.type() == "bigquery":
                    connection, client = adapter.connections.add_query(stmt)
                    ...
                else:  # We assume ~SqlAlchemy based as is the case for most adapters
                    connection, cursor = adapter.connections.add_query(stmt)
                    n_records = 0
                    records = []
                    if cursor.description is not None:
                        column_names = [col[0] for col in cursor.description]
                        for row in cursor:
                            # A nice, efficient, and often lazy stream of tuples
                            # Perfect for our memory footprint, circumventing uneeded Agate...
                            records.append(row)
                            n_records += 1
                            if n_records >= BATCH_SIZE:
                                write_batch(records, column_names, ref_parts)
                                records = []
                                n_records = 0
                        else:  # Ingest remainder
                            if n_records >= 0:
                                write_batch(records, column_names, ref_parts)
                                records = []
                                n_records = 0

