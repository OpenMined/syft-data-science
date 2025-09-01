import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

from syft_datasets.app.dependencies import get_dataset_manager
from syft_datasets.dataset import Dataset
from syft_datasets.dataset_manager import SyftDatasetManager

main_router = APIRouter()
dataset_router = APIRouter()


@main_router.get("/health")
def health():
    return {"status": "ok"}


class GetAllResponse(BaseModel):
    datasets: list[Dataset]
    total: int
    limit: int | None = None
    offset: int | None = None


@dataset_router.get("/datasets")
def get_all_datasets(
    dataset_manager: SyftDatasetManager = Depends(get_dataset_manager),
    datasite: str | None = Query(None, description="Filter datasets by datasite"),
    limit: int | None = Query(
        None, description="Limit the number of datasets returned"
    ),
    offset: int | None = Query(None, description="Offset for pagination"),
    order_by: str | None = Query(None, description="Field to order datasets by"),
    sort_order: str | None = Query("asc", description="Sort order: 'asc' or 'desc'"),
) -> GetAllResponse:
    """
    Retrieve all datasets available in the dataset manager with optional filters.
    """
    try:
        datasets = dataset_manager.get_all(
            datasite=datasite,
            order_by=order_by,
            sort_order=sort_order,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while retrieving datasets: {str(e)}",
        )

    total_datasets = len(datasets)
    if offset is not None:
        datasets = datasets[offset:]
    if limit is not None:
        datasets = datasets[:limit]

    return GetAllResponse(
        datasets=datasets,
        total=total_datasets,
        limit=limit,
        offset=offset,
    )


@dataset_router.get("/datasets/{datasite}/{dataset_name}")
def get_dataset(
    name: str,
    datasite: str,
    dataset_manager: SyftDatasetManager = Depends(get_dataset_manager),
) -> Dataset:
    """
    Retrieve a specific dataset by name and datasite.
    """
    try:
        dataset = dataset_manager.get(name=name, datasite=datasite)
        return dataset
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while retrieving the dataset: {str(e)}",
        )


@dataset_router.post("/datasets")
async def create_dataset(
    name: Annotated[str, Form(description="Name of the dataset")],
    mock_file: Annotated[
        UploadFile, File(description="Mock dataset file or folder (zipped)")
    ],
    private_file: Annotated[
        UploadFile, File(description="Private dataset file or folder (zipped)")
    ],
    readme_file: Annotated[
        UploadFile | None, File(description="Optional README file")
    ] = None,
    summary: Annotated[str | None, Form(description="Summary of the dataset")] = None,
    tags: Annotated[list[str] | None, Form(description="Tags for the dataset")] = None,
    dataset_manager: SyftDatasetManager = Depends(get_dataset_manager),
) -> Dataset:
    """
    Create a new dataset with file uploads for mock data, private data, and an optional README.
    Supports zipped folders for mock and private data.
    """
    try:
        # Create temporary directories for the uploaded files
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_path = Path(temp_dir) / "mock"
            private_path = Path(temp_dir) / "private"
            readme_path = Path(temp_dir) / "README.md" if readme_file else None

            # Save and extract mock file
            mock_temp_file = Path(temp_dir) / mock_file.filename
            with mock_temp_file.open("wb") as f:
                f.write(await mock_file.read())
            if zipfile.is_zipfile(mock_temp_file):
                with zipfile.ZipFile(mock_temp_file, "r") as zip_ref:
                    zip_ref.extractall(mock_path)
            else:
                shutil.move(mock_temp_file, mock_path)

            # Save and extract private file
            private_temp_file = Path(temp_dir) / private_file.filename
            with private_temp_file.open("wb") as f:
                f.write(await private_file.read())
            if zipfile.is_zipfile(private_temp_file):
                with zipfile.ZipFile(private_temp_file, "r") as zip_ref:
                    zip_ref.extractall(private_path)
            else:
                shutil.move(private_temp_file, private_path)

            # Save README file if provided
            if readme_file:
                with readme_path.open("wb") as f:
                    f.write(await readme_file.read())

            # Use the dataset manager to create the dataset
            dataset = dataset_manager.create(
                name=name,
                mock_path=mock_path,
                private_path=private_path,
                summary=summary,
                readme_path=readme_path,
                tags=tags,
            )

            return dataset

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while creating the dataset: {str(e)}",
        )
