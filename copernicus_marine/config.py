
from .transformer import (
    CopernicusOceanSeaSurfaceHeightTransformer,
    CopernicusOceanTemp0p5DepthTransformer,
    CopernicusOceanTemp1p5DepthTransformer,
    CopernicusOceanTemp6p5DepthTransformer,
    CopernicusOceanSalinity0p5DepthTransformer,
    CopernicusOceanSalinity1p5DepthTransformer,
    CopernicusOceanSalinity2p6DepthTransformer,
    CopernicusOceanSalinity25DepthTransformer,
    CopernicusOceanSalinity109DepthTransformer
)
from .combiner import (
    CopernicusOceanSeaSurfaceHeightCombiner,
    CopernicusOceanTemp0p5DepthCombiner,
    CopernicusOceanTemp1p5DepthCombiner,
    CopernicusOceanTemp6p5DepthCombiner,
    CopernicusOceanSalinity0p5DepthCombiner,
    CopernicusOceanSalinity1p5DepthCombiner,
    CopernicusOceanSalinity2p6DepthCombiner,
    CopernicusOceanSalinity25DepthCombiner,
    CopernicusOceanSalinity109DepthCombiner

)
from .base_values import (
    CopernicusOceanSeaSurfaceHeightValues,
    CopernicusOceanTemp0p5DepthValues,
    CopernicusOceanTemp1p5DepthValues,
    CopernicusOceanTemp6p5DepthValues,
    CopernicusOceanSalinity0p5DepthValues,
    CopernicusOceanSalinity1p5DepthValues,
    CopernicusOceanSalinity2p6DepthValues,
    CopernicusOceanSalinity25DepthValues,
    CopernicusOceanSalinity109DepthValues
)
from .assessor import (
    CopernicusOceanSeaSurfaceHeightAssessor,
    CopernicusOceanTemp0p5DepthAssessor,
    CopernicusOceanTemp1p5DepthAssessor,
    CopernicusOceanTemp6p5DepthAssessor,
    CopernicusOceanSalinity0p5DepthAssessor,
    CopernicusOceanSalinity1p5DepthAssessor,
    CopernicusOceanSalinity2p6DepthAssessor,
    CopernicusOceanSalinity25DepthAssessor,
    CopernicusOceanSalinity109DepthAssessor
    )
from .fetcher import (
    CopernicusOceanSeaSurfaceHeight,
    CopernicusOceanTemp0p5Depth,
    CopernicusOceanTemp1p5Depth,
    CopernicusOceanTemp6p5Depth,
    CopernicusOceanSalinity0p5Depth,
    CopernicusOceanSalinity1p5Depth,
    CopernicusOceanSalinity2p6Depth,
    CopernicusOceanSalinity25Depth,
    CopernicusOceanSalinity109Depth
)
from .ipld_loader import (
    CopernicusOceanSeaSurfaceHeightLoader,
    CopernicusOceanTemp0p5DepthLoader,
    CopernicusOceanTemp1p5DepthLoader,
    CopernicusOceanTemp6p5DepthLoader,
    CopernicusOceanSalinity0p5DepthLoader,
    CopernicusOceanSalinity1p5DepthLoader,
    CopernicusOceanSalinity2p6DepthLoader,
    CopernicusOceanSalinity25DepthLoader,
    CopernicusOceanSalinity109DepthLoader
)


# Centralized config for each dataset
DATASET_CONFIG = {
    "sea-surface-height": {
        "dataset_values_class": CopernicusOceanSeaSurfaceHeightValues,
        "assessor_class": CopernicusOceanSeaSurfaceHeightAssessor,
        "fetcher_class": CopernicusOceanSeaSurfaceHeight,
        "combiner": CopernicusOceanSeaSurfaceHeightCombiner,
        "stac_loader_class": CopernicusOceanSeaSurfaceHeightLoader,
        "transformer_class": CopernicusOceanSeaSurfaceHeightTransformer,
    },
    "temperature-0p5-depth": {
        "dataset_values_class": CopernicusOceanTemp0p5DepthValues,
        "assessor_class": CopernicusOceanTemp0p5DepthAssessor,
        "fetcher_class": CopernicusOceanTemp0p5Depth,
        "combiner": CopernicusOceanTemp0p5DepthCombiner,
        "stac_loader_class": CopernicusOceanTemp0p5DepthLoader,
        "transformer_class": CopernicusOceanTemp0p5DepthTransformer,
    },
    "temperature-1p5-depth": {
        "dataset_values_class": CopernicusOceanTemp1p5DepthValues,
        "assessor_class": CopernicusOceanTemp1p5DepthAssessor,
        "fetcher_class": CopernicusOceanTemp1p5Depth,
        "combiner": CopernicusOceanTemp1p5DepthCombiner,
        "stac_loader_class": CopernicusOceanTemp1p5DepthLoader,
        "transformer_class": CopernicusOceanTemp1p5DepthTransformer,
    },
    "temperature-6p5-depth": {
        "dataset_values_class": CopernicusOceanTemp6p5DepthValues,
        "assessor_class": CopernicusOceanTemp6p5DepthAssessor,
        "fetcher_class": CopernicusOceanTemp6p5Depth,
        "combiner": CopernicusOceanTemp6p5DepthCombiner,
        "stac_loader_class": CopernicusOceanTemp6p5DepthLoader,
        "transformer_class": CopernicusOceanTemp6p5DepthTransformer,
    },
    "salinity-0p5-depth": {
        "dataset_values_class": CopernicusOceanSalinity0p5DepthValues,
        "assessor_class": CopernicusOceanSalinity0p5DepthAssessor,
        "fetcher_class": CopernicusOceanSalinity0p5Depth,
        "combiner": CopernicusOceanSalinity0p5DepthCombiner,
        "stac_loader_class": CopernicusOceanSalinity0p5DepthLoader,
        "transformer_class": CopernicusOceanSalinity0p5DepthTransformer,
    },
    "salinity-1p5-depth": {
        "dataset_values_class": CopernicusOceanSalinity1p5DepthValues,
        "assessor_class": CopernicusOceanSalinity1p5DepthAssessor,
        "fetcher_class": CopernicusOceanSalinity1p5Depth,
        "combiner": CopernicusOceanSalinity1p5DepthCombiner,
        "stac_loader_class": CopernicusOceanSalinity1p5DepthLoader,
        "transformer_class": CopernicusOceanSalinity1p5DepthTransformer,
    },
    "salinity-2p6-depth": {
        "dataset_values_class": CopernicusOceanSalinity2p6DepthValues,
        "assessor_class": CopernicusOceanSalinity2p6DepthAssessor,
        "fetcher_class": CopernicusOceanSalinity2p6Depth,
        "combiner": CopernicusOceanSalinity2p6DepthCombiner,
        "stac_loader_class": CopernicusOceanSalinity2p6DepthLoader,
        "transformer_class": CopernicusOceanSalinity2p6DepthTransformer,
    },
    "salinity-25-depth": {
        "dataset_values_class": CopernicusOceanSalinity25DepthValues,
        "assessor_class": CopernicusOceanSalinity25DepthAssessor,
        "fetcher_class": CopernicusOceanSalinity25Depth,
        "combiner": CopernicusOceanSalinity25DepthCombiner,
        "stac_loader_class": CopernicusOceanSalinity25DepthLoader,
        "transformer_class": CopernicusOceanSalinity25DepthTransformer,
    },
    "salinity-109-depth": {
        "dataset_values_class": CopernicusOceanSalinity109DepthValues,
        "assessor_class": CopernicusOceanSalinity109DepthAssessor,
        "fetcher_class": CopernicusOceanSalinity109Depth,
        "combiner": CopernicusOceanSalinity109DepthCombiner,
        "stac_loader_class": CopernicusOceanSalinity109DepthLoader,
        "transformer_class": CopernicusOceanSalinity109DepthTransformer,
    }

}