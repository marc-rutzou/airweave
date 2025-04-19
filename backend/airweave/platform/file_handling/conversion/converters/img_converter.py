"""Image converter module for converting images to markdown.

This module provides functionality to convert images to markdown text
using Mistral OCR, metadata extraction, and optional LLM-based image description.
"""

import base64
import json
import mimetypes
import os
import shutil
import subprocess
from typing import Any, Dict, Optional, Union

from airweave.core.config import settings
from airweave.core.logging import logger
from airweave.platform.file_handling.conversion._base import (
    DocumentConverter,
    DocumentConverterResult,
)

# Initialize Mistral client if API key is available
mistral_client = None
if hasattr(settings, "MISTRAL_API_KEY") and settings.MISTRAL_API_KEY:
    from mistralai import Mistral

    mistral_client = Mistral(api_key=settings.MISTRAL_API_KEY)

# Maximum file size for Mistral OCR (50MB in bytes)
MAX_MISTRAL_FILE_SIZE = 50 * 1024 * 1024


class AsyncImageConverter(DocumentConverter):
    """Converts images to markdown.

    Via Mistral OCRmetadata extraction and optional LLM description.
    """

    def __init__(self):
        """Initialize the image converter with Mistral client if API key is available."""
        self.mistral_client = mistral_client

    async def convert(self, local_path: str, **kwargs: Any) -> Union[None, DocumentConverterResult]:
        """Convert an image to markdown.

        Args:
            local_path: Path to the image file
            **kwargs: Additional arguments passed to converters

        Returns:
            AsyncDocumentConverterResult containing the markdown text
        """
        # Bail if not supported image type
        extension = kwargs.get("file_extension", "")
        if not self._is_supported_image(extension):
            return None

        md_content = ""

        # Try Mistral OCR first if available and file size is under 50MB
        ocr_result = await self._try_mistral_ocr(local_path)
        if ocr_result:
            return DocumentConverterResult(title=None, text_content=ocr_result.strip())

        # Add metadata if exiftool is available
        md_content = await self._extract_metadata_content(local_path)

        # Try describing the image with LLM if client is provided
        llm_description = await self._try_llm_description(local_path, extension, **kwargs)
        if llm_description:
            md_content += f"\n# Description:\n{llm_description.strip()}\n"

        return DocumentConverterResult(title=None, text_content=md_content.strip())

    def _is_supported_image(self, extension: str) -> bool:
        """Check if the file extension is supported.

        Args:
            extension: File extension including the dot

        Returns:
            True if supported, False otherwise
        """
        return extension.lower() in [".jpg", ".jpeg", ".png"]

    async def _try_mistral_ocr(self, local_path: str) -> Optional[str]:
        """Try to process the image with Mistral OCR if available.

        Args:
            local_path: Path to the image file

        Returns:
            OCR text result or None if unavailable or failed
        """
        if not self.mistral_client:
            return None

        try:
            file_size = os.path.getsize(local_path)
            if file_size <= MAX_MISTRAL_FILE_SIZE:
                ocr_text = await self._process_with_mistral_ocr(local_path)
                if ocr_text:
                    return ocr_text
        except Exception as e:
            logger.error(f"Error processing image with Mistral OCR: {str(e)}")
            logger.info("Falling back to metadata extraction and LLM description")

        return None

    async def _extract_metadata_content(self, local_path: str) -> str:
        """Extract metadata from the image and format as markdown.

        Args:
            local_path: Path to the image file

        Returns:
            Formatted metadata content
        """
        md_content = ""
        metadata = await self._get_metadata(local_path)
        if metadata:
            for field in [
                "ImageSize",
                "Title",
                "Caption",
                "Description",
                "Keywords",
                "Artist",
                "Author",
                "DateTimeOriginal",
                "CreateDate",
                "GPSPosition",
            ]:
                if field in metadata:
                    md_content += f"{field}: {metadata[field]}\n"

        return md_content

    async def _try_llm_description(
        self, local_path: str, extension: str, **kwargs
    ) -> Optional[str]:
        """Try to get a description of the image using an LLM.

        Args:
            local_path: Path to the image file
            extension: File extension
            **kwargs: Additional arguments that might contain LLM client and model

        Returns:
            Description text or None if unavailable
        """
        llm_client = kwargs.get("llm_client")
        llm_model = kwargs.get("llm_model")

        if not (llm_client and llm_model):
            return None

        return await self._get_llm_description(
            local_path, extension, llm_client, llm_model, prompt=kwargs.get("llm_prompt")
        )

    async def _process_with_mistral_ocr(self, image_path: str) -> str:
        """Process image using Mistral OCR.

        Args:
            image_path: Path to the image file

        Returns:
            String containing the OCR results in markdown format

        Raises:
            Exception: If Mistral OCR processing fails
        """
        logger.info(f"Using Mistral OCR to process image: {image_path}")

        # Upload file to Mistral
        with open(image_path, "rb") as file:
            uploaded_image = self.mistral_client.files.upload(
                file={
                    "file_name": os.path.basename(image_path),
                    "content": file,
                },
                purpose="ocr",
            )

        # Get signed URL for accessing the file
        signed_url = self.mistral_client.files.get_signed_url(file_id=uploaded_image.id)

        # Process file with OCR
        ocr_response = self.mistral_client.ocr.process(
            model="mistral-ocr-latest",
            document={
                "type": "image_url",
                "image_url": signed_url.url,
            },
        )

        # Extract markdown content from response
        md_content = ""

        # If there are pages in the response (like for multi-page images)
        if hasattr(ocr_response, "pages") and ocr_response.pages:
            for page in ocr_response.pages:
                md_content += f"{page.markdown}"
        # If it's a simple image with direct markdown content
        elif hasattr(ocr_response, "markdown"):
            md_content = ocr_response.markdown

        return md_content.strip()

    async def _get_metadata(self, local_path: str) -> Optional[Dict[str, Any]]:
        """Get image metadata using exiftool if available."""
        exiftool = shutil.which("exiftool")
        if not exiftool:
            return None

        try:
            result = subprocess.run(
                [exiftool, "-json", local_path], capture_output=True, text=True
            ).stdout
            return json.loads(result)[0]
        except Exception:
            return None

    async def _get_llm_description(
        self, local_path: str, extension: str, client: Any, model: str, prompt: Optional[str] = None
    ) -> str:
        """Get image description from LLM if available."""
        if not prompt:
            prompt = "Write a detailed caption for this image."

        # Convert image to data URI
        with open(local_path, "rb") as image_file:
            content_type, _ = mimetypes.guess_type("dummy" + extension)
            if not content_type:
                content_type = "image/jpeg"
            image_base64 = base64.b64encode(image_file.read()).decode("utf-8")
            data_uri = f"data:{content_type};base64,{image_base64}"

        # Prepare messages for LLM
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_uri}},
                ],
            }
        ]

        # Get response from LLM
        response = await client.chat.completions.create(model=model, messages=messages)
        return response.choices[0].message.content
