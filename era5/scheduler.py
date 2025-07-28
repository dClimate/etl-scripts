import asyncio
import json
from pathlib import Path
import click
import sys
import warnings
from datetime import datetime, timedelta
# Add the project root to the Python path to allow for package imports
# This is necessary if you run the script from a different directory
sys.path.append(str(Path(__file__).parent.resolve()))

from era5.processor import append_latest
from etl_scripts.grabbag import eprint

# Define the path to your central CID tracking file
CIDS_FILE = Path(__file__).parent / "cids.json"
scratchspace: Path = (Path(__file__).parent/ "scratchspace").absolute()


async def update_dataset(dataset, cid, finalization_only):
    """
    A wrapper around the append_latest function to handle the update
    for a single dataset type (finalized or non-finalized).
    """
    status = "finalized" if finalization_only else "non-finalized"
    eprint(f"\n▶️  Checking for updates: {dataset} ({status})")
    eprint(f"   Current CID: {cid}")

    # Don't proceed if the dataset hasn't been initialized yet
    if not cid or cid == "null":
        eprint(f"   Skipping {dataset} ({status}) as no initial CID is present.")
        return None

    try:
        # The append_latest function will check for new data and act accordingly
        new_cid = await append_latest(
            dataset=dataset,
            cid=cid,
            end_date=None,  # Pass None to let the function find the latest date
            gateway_uri_stem=None,
            rpc_uri_stem=None,
            finalization_only=finalization_only,
            api_key=None,
        )
        
        # append_latest returns the original CID if no update occurred.
        if new_cid and new_cid != cid:
            eprint(f"✅ Update successful for {dataset} ({status}). New CID: {new_cid}")
            return new_cid
        else:
            eprint(f"   No new data found for {dataset} ({status}). CID remains unchanged.")
            return cid

    except Exception as e:
        eprint(f"❌ ERROR updating {dataset} ({status}): {e}")
        # Return the original CID to avoid losing the reference on failure
        return cid

async def orchestrator():
    """
    Main function to orchestrate the entire daily update process.
    """
    eprint("--- Starting Daily ERA5 Append Job ---")

    # Load the source of truth for all dataset CIDs
    try:
        with open(CIDS_FILE, 'r') as f:
            cids_data = json.load(f)
    except FileNotFoundError:
        eprint(f"FATAL: Could not find cids file at {CIDS_FILE}")
        sys.exit(1)
    except json.JSONDecodeError:
        eprint(f"FATAL: Could not parse JSON from {CIDS_FILE}")
        sys.exit(1)

    updated_cids = cids_data.copy()
    has_updates = False


    # Iterate through each dataset defined in the cids.json file
    for dataset, data in cids_data.get("era5", {}).items():
        # --- 1. Update Finalized CID ---
        original_finalized_cid = data.get("finalized")
        latest_checked_date = data.get("finalization_date_checked")
        if latest_checked_date != "null":
            latest_checked_date = datetime.fromisoformat(latest_checked_date)
        else:
            latest_checked_date = datetime.min  # Default to a very old date if not set
        
        # If the latest checked date is more than 2 weeks ago, we consider it stale
        if datetime.now() - timedelta(days=14) > latest_checked_date:
            eprint(f"   Finalization date for {dataset} is stale. Checking for updates...")
            # Update the finalized dataset
            new_finalized_cid = await update_dataset(
                dataset=dataset,
                cid=original_finalized_cid,
                finalization_only=True, # IMPORTANT
            )
            updated_cids["era5"][dataset]["finalization_date_checked"] = datetime.now().isoformat()
            if new_finalized_cid and new_finalized_cid != original_finalized_cid:
                updated_cids["era5"][dataset]["finalized"] = new_finalized_cid
                has_updates = True
            
            # save the json for now
            with open(CIDS_FILE, 'w') as f:
                json.dump(updated_cids, f, indent=2)
            

        # --- 2. Update Non-Finalized CID ---
        original_non_finalized_cid = data.get("non-finalized")
        new_non_finalized_cid = await update_dataset(
            dataset=dataset,
            cid=original_non_finalized_cid,
            finalization_only=False, # IMPORTANT
        )
        if new_non_finalized_cid and new_non_finalized_cid != original_non_finalized_cid:
            updated_cids["era5"][dataset]["non-finalized"] = new_non_finalized_cid
            has_updates = True
        # save the json for now
        with open(CIDS_FILE, 'w') as f:
            json.dump(updated_cids, f, indent=2)

        # Delete all files from scratchspace that were used in this run
        # File names follor dataset_YYYYMM and end with .grib or .grib.<extension>
        # for file in scratchspace.glob(f"{dataset}_*.grib*"):
        #     try:
        #         file.unlink()
        #         eprint(f"   Deleted scratchspace file: {file.name}")
        #     except FileNotFoundError:
        #         eprint(f"   File not found for deletion: {file.name}")
        #     except Exception as e:
        #         eprint(f"   Error deleting file {file.name}: {e}")


    # After checking all datasets, write the changes back to the file if any
    if has_updates:
        eprint(f"\n💾 Updates were made. Writing new CIDs back to {CIDS_FILE}...")
        try:
            with open(CIDS_FILE, 'w') as f:
                json.dump(updated_cids, f, indent=2)
            eprint("   ✅ cids.json updated successfully.")
        except IOError as e:
            eprint(f"   ❌ FATAL: Could not write updated CIDs to file: {e}")
    else:
        eprint("\nNo updates were needed for any dataset. cids.json remains unchanged.")

    eprint("\n--- Daily ERA5 Append Job Finished ---")

@click.command()
def run_daily_updater():
    """CLI entry point for the daily updater script."""
    with warnings.catch_warnings():
        # Suppress warnings that are not critical for this operation
        warnings.filterwarnings("ignore", category=UserWarning, message="Consolidated metadata is currently not part")
        asyncio.run(orchestrator())

if __name__ == "__main__":
    run_daily_updater()