from fastapi import Request

from syft_datasets.dataset_manager import SyftDatasetManager


def get_dataset_manager(request: Request) -> SyftDatasetManager:
    return request.app.state.dataset_manager
