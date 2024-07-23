for arg in "$@"; do
    case "$arg" in
        nc)
            find . -type f -name ".nc" -delete
            ;;
        download-links)
            find . -type f -name "download-links.txt" -delete
            ;;
        zarr)
            find . -type d -name "*.zarr" -exec rm -rf {} +
            ;;
        misc)
            find . -type d -name ".ruff_cache" -exec rm -rf {} +
            find . -type f -name "*.cid" -delete
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            ;;
    esac
done
