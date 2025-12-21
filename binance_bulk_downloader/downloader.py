"""
Binance Bulk Downloader
"""

# imports for gcs support
import io
from google.cloud import storage

# import standard libraries
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from xml.etree import ElementTree
from zipfile import BadZipfile
from typing import Optional, List, Union

# import third-party libraries
import requests
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.text import Text

# import my libraries
from binance_bulk_downloader.exceptions import (
    BinanceBulkDownloaderDownloadError,
    BinanceBulkDownloaderParamsError,
)


class BinanceBulkDownloader:
    """
    Binance Bulk Downloader class for downloading historical data from Binance Vision.
    Supports all asset types (spot, USDT-M, COIN-M, options) and all data frequencies.
    """

    _CHUNK_SIZE = 100
    _BINANCE_DATA_S3_BUCKET_URL = (
        "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
    )
    _BINANCE_DATA_DOWNLOAD_BASE_URL = "https://data.binance.vision"
    _FUTURES_ASSET = ("um", "cm")
    _OPTIONS_ASSET = ("option",)
    _ASSET = ("spot",)
    _DATA_TYPE_BY_ASSET = {
        "um": {
            "daily": (
                "aggTrades",
                "bookDepth",
                "bookTicker",
                "indexPriceKlines",
                "klines",
                "liquidationSnapshot",
                "markPriceKlines",
                "metrics",
                "premiumIndexKlines",
                "trades",
            ),
            "monthly": (
                "aggTrades",
                "bookTicker",
                "fundingRate",
                "indexPriceKlines",
                "klines",
                "markPriceKlines",
                "premiumIndexKlines",
                "trades",
            ),
        },
        "cm": {
            "daily": (
                "aggTrades",
                "bookDepth",
                "bookTicker",
                "indexPriceKlines",
                "klines",
                "liquidationSnapshot",
                "markPriceKlines",
                "metrics",
                "premiumIndexKlines",
                "trades",
            ),
            "monthly": (
                "aggTrades",
                "bookTicker",
                "fundingRate",
                "indexPriceKlines",
                "klines",
                "markPriceKlines",
                "premiumIndexKlines",
                "trades",
            ),
        },
        "spot": {
            "daily": ("aggTrades", "klines", "trades"),
            "monthly": ("aggTrades", "klines", "trades"),
        },
        "option": {"daily": ("BVOLIndex", "EOHSummary")},
    }
    _DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE = (
        "klines",
        "markPriceKlines",
        "indexPriceKlines",
        "premiumIndexKlines",
    )
    _DATA_FREQUENCY = (
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1mo",
    )

    def __init__(
        self,
        gcs_bucket_name: str,
        filtr: Optional[str] = None,
        destination_dir=".",
        data_type="klines",
        data_frequency="1m",
        asset="um",
        timeperiod_per_file="daily",
        symbols: Optional[Union[str, List[str]]] = None,
    ) -> None:
        """
        Initialize BinanceBulkDownloader

        :param gcs_bucket_name: The name of the GCS bucket to upload to.
        :param filtr: Optional. A string in "YYYY-MM" format to filter downloads.
        :param destination_dir: Destination directory for downloaded files
        :param data_type: Type of data to download (klines, aggTrades, etc.)
        :param data_frequency: Frequency of data to download (1m, 1h, 1d, etc.)
        :param asset: Type of asset to download (um, cm, spot, option)
        :param timeperiod_per_file: Time period per file (daily, monthly)
        :param symbols: Optional. Symbol or list of symbols to download (e.g., "BTCUSDT" or ["BTCUSDT", "ETHUSDT"]).
                        If None or empty list is provided, all available symbols will be downloaded.
        """
        # GCS Initialization
        self._gcs_bucket_name = gcs_bucket_name
        self._storage_client = storage.Client()
        self._gcs_bucket = self._storage_client.bucket(self._gcs_bucket_name)
        self._filter = filtr


        # Original parameters
        self._destination_dir = destination_dir
        self._data_type = data_type
        self._data_frequency = data_frequency
        self._asset = asset
        self._timeperiod_per_file = timeperiod_per_file
        self._symbols = [symbols] if isinstance(symbols, str) else symbols
        self.marker = None
        self.is_truncated = True
        self.downloaded_list: list[str] = []
        self.console = Console()

    def _check_params(self) -> None:
        """
        Check params
        :return: None
        """
        # Check asset type first
        if self._asset not in self._ASSET + self._FUTURES_ASSET + self._OPTIONS_ASSET:
            raise BinanceBulkDownloaderParamsError(
                f"asset must be {self._ASSET + self._FUTURES_ASSET + self._OPTIONS_ASSET}."
            )

        # Check time period
        if self._timeperiod_per_file not in ["daily", "monthly"]:
            raise BinanceBulkDownloaderParamsError(
                "timeperiod_per_file must be daily or monthly."
            )

        # Check data frequency
        if self._data_frequency not in self._DATA_FREQUENCY:
            raise BinanceBulkDownloaderParamsError(
                f"data_frequency must be {self._DATA_FREQUENCY}."
            )

        # Check if asset exists in DATA_TYPE_BY_ASSET
        if self._asset not in self._DATA_TYPE_BY_ASSET:
            raise BinanceBulkDownloaderParamsError(
                f"asset {self._asset} is not supported."
            )

        # Check if timeperiod exists for the asset
        asset_data = self._DATA_TYPE_BY_ASSET.get(self._asset, {})
        if self._timeperiod_per_file not in asset_data:
            raise BinanceBulkDownloaderParamsError(
                f"timeperiod {self._timeperiod_per_file} is not supported for {self._asset}."
            )

        # Check data type
        valid_data_types = asset_data.get(self._timeperiod_per_file, [])
        if self._data_type not in valid_data_types:
            raise BinanceBulkDownloaderParamsError(
                f"data_type must be one of {valid_data_types}."
            )

        # Check 1s frequency restriction
        if self._data_frequency == "1s":
            if self._asset != "spot":
                raise BinanceBulkDownloaderParamsError(
                    f"data_frequency 1s is not supported for {self._asset}."
                )

    def _get_file_list_from_s3_bucket(self, prefix):
        """
        Get file list from s3 bucket
        :param prefix: s3 bucket prefix
        :return: list of files
        """
        files = []
        marker = None
        is_truncated = True
        MAX_DISPLAY_FILES = 5

        with Live(refresh_per_second=4) as live:
            status_text = Text(f"Getting file list: {prefix}")
            live.update(Panel(status_text, style="blue"))

            while is_truncated:
                params = {"prefix": prefix, "max-keys": 1000}
                if marker:
                    params["marker"] = marker

                response = requests.get(self._BINANCE_DATA_S3_BUCKET_URL, params=params)
                tree = ElementTree.fromstring(response.content)

                for content in tree.findall(
                    "{http://s3.amazonaws.com/doc/2006-03-01/}Contents"
                ):
                    key = content.find(
                        "{http://s3.amazonaws.com/doc/2006-03-01/}Key"
                    ).text
                    if key.endswith(".zip"):
                        # Filter by symbols if multiple symbols are specified
                        if isinstance(self._symbols, list) and len(self._symbols) > 1:
                            if any(symbol.upper() in key for symbol in self._symbols):
                                files.append(key)
                                marker = key
                        else:
                            files.append(key)
                            marker = key

                        # Update display (latest files and total count)
                        status_text.plain = f"Getting file list: {prefix}\nTotal files found: {len(files)}"
                        if files:
                            status_text.append("\n\nLatest files:")
                            for recent_file in files[-MAX_DISPLAY_FILES:]:
                                status_text.append(f"\n{recent_file}")
                        live.update(Panel(status_text, style="blue"))

                is_truncated_element = tree.find(
                    "{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated"
                )
                is_truncated = (
                    is_truncated_element is not None
                    and is_truncated_element.text.lower() == "true"
                )

            status_text.plain = (
                f"File list complete: {prefix}\nTotal files found: {len(files)}"
            )
            if files:
                status_text.append("\n\nLatest files:")
                for recent_file in files[-MAX_DISPLAY_FILES:]:
                    status_text.append(f"\n{recent_file}")
            live.update(Panel(status_text, style="green"))
            return files

    def _make_asset_type(self) -> str:
        """
        Convert asset to asset type
        :return:
        """
        if self._asset in "um":
            asset_type = "futures/um"
        elif self._asset in "cm":
            asset_type = "futures/cm"
        elif self._asset in self._OPTIONS_ASSET:
            asset_type = "option"
        elif self._asset in self._ASSET:
            asset_type = "spot"
        else:
            raise BinanceBulkDownloaderParamsError(
                "asset must be futures, options or spot."
            )
        return asset_type

    def _set_timeperiod_per_file(self, timeperiod_per_file) -> None:
        """
        Set timeperiod_per_file
        :param timeperiod_per_file: Time period per file (daily, monthly)
        :return:
        """
        self._timeperiod_per_file = timeperiod_per_file

    def _build_prefix(self) -> str:
        """
        Build prefix to download
        :return: s3 bucket prefix
        """
        url_parts = [
            "data",
            self._make_asset_type(),
            self._timeperiod_per_file,
            self._data_type,
        ]

        # If single symbol is specified, add it to the prefix
        if isinstance(self._symbols, list) and len(self._symbols) == 1:
            symbol = self._symbols[0].upper()
            url_parts.append(symbol)
            # For trades and aggTrades, add symbol directory
            if self._data_type in ["trades", "aggTrades"]:
                url_parts.append(symbol)
        elif isinstance(self._symbols, str):
            symbol = self._symbols.upper()
            url_parts.append(symbol)
            # For trades and aggTrades, add symbol directory
            if self._data_type in ["trades", "aggTrades"]:
                url_parts.append(symbol)

        # If data frequency is required and specified, add it to the prefix
        if (
            self._data_type in self._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE
            and self._data_frequency
        ):
            if isinstance(self._symbols, (str, list)):
                url_parts.append(f"{self._data_frequency}/")

        return "/".join(url_parts)

    def _download_gcs(self, prefix: str) -> None:
        """
        Executes download from Binance, unzips in-memory, and uploads the raw CSV
        to a landing path in GCS under a 'binance/' prefix.
        :param prefix: The file path from Binance (e.g., data/spot/daily/trades/BTCUSDT/...).
        """
        try:
            self._check_params()

            # 1. Construct the GCS destination path with the 'binance/' prefix.
            csv_destination_path = f"binance/{prefix.replace('.zip', '.csv')}"

            # 2. Check if the file already exists in GCS to avoid re-downloading.
            blob = self._gcs_bucket.blob(csv_destination_path)
            if blob.exists():
                return

            # 3. Download the ZIP file into memory.
            url = f"{self._BINANCE_DATA_DOWNLOAD_BASE_URL}/{prefix}"
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                # Handle specific 404 Not Found errors gracefully, as some daily files may not exist.
                if (
                    isinstance(e, requests.exceptions.HTTPError)
                    and e.response.status_code == 404
                ):
                    return
                # Raise for all other network or HTTP errors.
                raise BinanceBulkDownloaderDownloadError(
                    f"Download error for {url}: {str(e)}"
                )

            # 4. Unzip the content in memory.
            try:
                with io.BytesIO(response.content) as zip_buffer:
                    with zipfile.ZipFile(zip_buffer) as zf:
                        # Handle empty zip files, which can occasionally occur.
                        if not zf.namelist():
                            return
                        csv_filename = zf.namelist()[0]
                        csv_content = zf.read(csv_filename)
            except BadZipfile:
                # If the downloaded file is not a valid zip, skip it.
                return

            # 5. Upload the extracted CSV content to GCS.
            blob.upload_from_string(csv_content, content_type="text/csv")

        except Exception as e:
            # Catch any other unexpected errors.
            if not isinstance(e, BinanceBulkDownloaderDownloadError):
                raise BinanceBulkDownloaderDownloadError(
                    f"An unexpected error occurred for prefix {prefix}: {str(e)}"
                )
            raise

    def _download(self, prefix) -> None:
        """
        Execute download
        :param prefix: s3 bucket prefix
        :return: None
        """
        try:
            self._check_params()
            zip_destination_path = os.path.join(self._destination_dir, prefix)
            csv_destination_path = os.path.join(
                self._destination_dir, prefix.replace(".zip", ".csv")
            )

            # Make directory if not exists
            if not os.path.exists(os.path.dirname(zip_destination_path)):
                try:
                    os.makedirs(os.path.dirname(zip_destination_path))
                except (PermissionError, OSError) as e:
                    raise BinanceBulkDownloaderDownloadError(
                        f"Directory creation error: {str(e)}"
                    )

            # Don't download if already exists
            if os.path.exists(csv_destination_path):
                return

            url = f"{self._BINANCE_DATA_DOWNLOAD_BASE_URL}/{prefix}"

            try:
                response = requests.get(url)
                response.raise_for_status()
            except (
                requests.exceptions.RequestException,
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                raise BinanceBulkDownloaderDownloadError(f"Download error: {str(e)}")

            try:
                with open(zip_destination_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
            except OSError as e:
                raise BinanceBulkDownloaderDownloadError(f"File write error: {str(e)}")

            try:
                unzipped_path = "/".join(zip_destination_path.split("/")[:-1])
                with zipfile.ZipFile(zip_destination_path) as existing_zip:
                    existing_zip.extractall(
                        csv_destination_path.replace(
                            csv_destination_path, unzipped_path
                        )
                    )
            except BadZipfile as e:
                if os.path.exists(zip_destination_path):
                    os.remove(zip_destination_path)
                raise BinanceBulkDownloaderDownloadError(
                    f"Bad Zip File: {zip_destination_path}"
                )
            except OSError as e:
                if os.path.exists(zip_destination_path):
                    os.remove(zip_destination_path)
                raise BinanceBulkDownloaderDownloadError(f"Unzip error: {str(e)}")

            # Delete zip file
            try:
                os.remove(zip_destination_path)
            except OSError as e:
                raise BinanceBulkDownloaderDownloadError(
                    f"File removal error: {str(e)}"
                )

        except Exception as e:
            if not isinstance(e, BinanceBulkDownloaderDownloadError):
                raise BinanceBulkDownloaderDownloadError(f"Unexpected error: {str(e)}")
            raise

    @staticmethod
    def make_chunks(lst, n) -> list:
        """
        Make chunks
        :param lst: Raw list
        :param n: size of chunk
        :return: list of chunks
        """
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    def run_download(self):
        """
        Download concurrently
        :return: None
        """
        self.console.print(
            Panel(f"Starting download for {self._data_type}", style="blue bold")
        )

        file_list = []
        # Handle multiple symbols by getting each symbol's files separately
        if isinstance(self._symbols, list) and len(self._symbols) > 1:
            original_symbols = self._symbols
            for symbol in original_symbols:
                self._symbols = symbol  # Temporarily set to single symbol
                symbol_files = self._get_file_list_from_s3_bucket(self._build_prefix())
                file_list.extend(symbol_files)
            self._symbols = original_symbols  # Restore original symbols
        else:
            file_list = self._get_file_list_from_s3_bucket(self._build_prefix())

        if self._filter:
            self.console.print(
                f"Applying filter for month: [yellow]{self._filter}[/yellow]"
            )
            original_count = len(file_list)
            file_list = [f for f in file_list if self._filter in f]
            self.console.print(
                f"Filtered file list from {original_count} to {len(file_list)} files."
            )

        if not file_list:
            self.console.print(
                "[bold red]No files found matching the criteria. Exiting.[/bold red]"
            )
            return

        # Filter by data frequency only if not already filtered by prefix
        if (
            self._data_type in self._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE
            and not isinstance(self._symbols, (str, list))
        ):
            file_list = [
                prefix
                for prefix in file_list
                if prefix.count(self._data_frequency) == 2
            ]

        # Create progress display
        with Live(refresh_per_second=4) as live:
            status = Text()
            chunks = self.make_chunks(file_list, self._CHUNK_SIZE)
            total_chunks = len(chunks)

            # Download files in chunks
            for chunk_index, prefix_chunk in enumerate(chunks, 1):
                with ThreadPoolExecutor() as executor:
                    futures = []
                    for prefix in prefix_chunk:
                        future = executor.submit(self._download_gcs, prefix)
                        futures.append((future, prefix))

                    # Update status as files complete
                    for future, prefix in futures:
                        try:
                            future.result()
                            progress = (
                                (len(self.downloaded_list) + 1) / len(file_list) * 100
                            )
                            status.plain = f"[{chunk_index}/{total_chunks}] Progress: {progress:.1f}% | Latest: {os.path.basename(prefix)}"
                            live.update(status)
                        except Exception as e:
                            status.plain = f"Error: {str(e)}"
                            live.update(status)

                self.downloaded_list.extend(prefix_chunk)


def main():
    """
    source venv/bin/activate
    python -m binance_bulk_downloader.downloader
    deactivate
    """
    bucket_name = os.getenv("GCP_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("Environment variable GCP_BUCKET_NAME not set.")

    filtr = os.getenv("BINANCE_BULK_DOWNLOADER_YYYY_MM_FILTER")
    if not filtr:
        filtr = None
    data_type = os.getenv("BINANCE_BULK_DOWNLOADER_DATA_TYPE", "klines")

    data_frequencies = [
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1mo",
    ]

    for data_frequency in data_frequencies:
        downloader = BinanceBulkDownloader(
            gcs_bucket_name=bucket_name,
            data_frequency=data_frequency,
            filtr=filtr,
            data_type=data_type,
            asset="spot",
            timeperiod_per_file="monthly",
            symbols=[
                "BTCUSDT",
                "BNBUSDT",
                "ETHUSDT",
                "SOLUSDT",
                "XRPUSDT",
                "TRXUSDT",
                "ADAUSDT",
                "XLMUSDT",
            ],
        )
        downloader.run_download()


if __name__ == "__main__":
    main()
