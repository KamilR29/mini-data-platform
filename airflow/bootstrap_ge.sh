#!/usr/bin/env bash
set -euo pipefail

GE_DIR="/work/great_expectations"
GE_YML="${GE_DIR}/great_expectations.yml"

if [ -f "${GE_YML}" ]; then
  echo "[GE] Project exists: ${GE_YML}"
  exit 0
fi

echo "[GE] Creating minimal Great Expectations project in ${GE_DIR} ..."

mkdir -p "${GE_DIR}/expectations" \
         "${GE_DIR}/checkpoints" \
         "${GE_DIR}/plugins" \
         "${GE_DIR}/uncommitted/data_docs" \
         "${GE_DIR}/uncommitted/validations"

cat > "${GE_YML}" << 'YAML'
config_version: 3.0
plugins_directory: plugins/

stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

  validation_results_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: checkpoints/

expectations_store_name: expectations_store
validation_results_store_name: validation_results_store
checkpoint_store_name: checkpoint_store

data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: false
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
YAML

echo "[GE] Done."
