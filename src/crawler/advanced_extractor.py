"""10-K 파일링을 위한 고급 텍스트 추출 엔진."""

import asyncio
import cssutils
import json
import logging
import numpy as np
import os
import pandas as pd
import re
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
from pathlib import Path

from bs4 import BeautifulSoup
from html.parser import HTMLParser
from loguru import logger

# cssutils 경고 메시지 억제
cssutils.log.setLevel(logging.CRITICAL)

# 깊은 파싱을 위한 재귀 한계 설정
sys.setrecursionlimit(30000)


class HtmlStripper(HTMLParser):
    """향상된 HTML 태그 스트리퍼."""
    
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []
    
    def handle_data(self, data: str) -> None:
        self.fed.append(data)
    
    def get_data(self) -> str:
        return "".join(self.fed)
    
    def strip_tags(self, html: str) -> str:
        self.fed = []  # Reset for reuse
        self.feed(html)
        return self.get_data()


class AdvancedTextExtractor:
    """Advanced text extraction engine with enhanced parsing capabilities."""
    
    def __init__(self, remove_tables: bool = False):
        self.remove_tables = remove_tables
        self.html_stripper = HtmlStripper()
        
        # Enhanced item list for 10-K sections
        self.items_list = [
            "1", "1A", "1B", "2", "3", "4", "5", "6", "7", "7A",
            "8", "9", "9A", "9B", "10", "11", "12", "13", "14", "15"
        ]
        
        # Regex flags for consistent matching
        self.regex_flags = re.IGNORECASE | re.DOTALL | re.MULTILINE
    
    def extract_sections_from_content(
        self,
        html_content: str,
        items_to_extract: Optional[List[str]] = None
    ) -> Dict[str, str]:
        """Extract specified sections from 10-K HTML content."""
        if items_to_extract is None:
            items_to_extract = ["1", "1A", "7", "7A"]
        
        logger.info(f"Extracting sections: {items_to_extract}")
        
        try:
            # Parse HTML and detect format
            soup = BeautifulSoup(html_content, 'html.parser')
            is_html = self._detect_html_format(soup)
            
            # Remove tables if requested
            if self.remove_tables:
                content = self._remove_html_tables(soup if is_html else html_content, is_html)
            else:
                content = soup if is_html else html_content
            
            # Extract text and clean
            text = self._strip_html(str(content))
            text = self._clean_text(text)
            
            # Extract sections
            sections = {}
            positions = []
            
            for i, item_index in enumerate(self.items_list):
                if item_index in items_to_extract:
                    next_item_list = self.items_list[i + 1:]
                    
                    section_text, positions = self._parse_item_section(
                        text, item_index, next_item_list, positions
                    )
                    
                    if section_text.strip():
                        sections[f"item_{item_index}"] = self._remove_multiple_lines(section_text)
                        logger.debug(f"Extracted Item {item_index}: {len(section_text)} characters")
            
            logger.info(f"Successfully extracted {len(sections)} sections")
            return sections
            
        except Exception as e:
            logger.error(f"Error extracting sections: {e}")
            return {}
    
    def _detect_html_format(self, soup: BeautifulSoup) -> bool:
        """Detect if content is HTML or plain text."""
        return bool(soup.find("td")) and bool(soup.find("tr"))
    
    def _strip_html(self, html_content: str) -> str:
        """Strip HTML tags with enhanced formatting preservation."""
        # Preserve structure with newlines
        html_content = re.sub(r"(<\s*/\s*(div|tr|p|li|h[1-6])\s*>)", r"\1\n\n", html_content)
        html_content = re.sub(r"(<br\s*>|<br\s*/>)", r"\1\n\n", html_content)
        html_content = re.sub(r"(<\s*/\s*(th|td)\s*>)", r" \1 ", html_content)
        
        # Strip remaining HTML tags
        return self.html_stripper.strip_tags(html_content)
    
    def _clean_text(self, text: str) -> str:
        """Advanced text cleaning with enhanced pattern recognition."""
        # Character substitutions
        substitutions = {
            r'[\xa0]': ' ',
            r'[\u200b]': ' ',
            r'[\x91]': "'",
            r'[\x92]': "'",
            r'[\x93]': '"',
            r'[\x94]': '"',
            r'[\x95]': '•',
            r'[\x96]': '-',
            r'[\x97]': '-',
            r'[\x98]': '˜',
            r'[\x99]': '™',
            r'[\u2010\u2011\u2012\u2013\u2014\u2015]': '-'
        }
        
        for pattern, replacement in substitutions.items():
            text = re.sub(pattern, replacement, text)
        
        # Fix broken section headers
        def remove_whitespace(match):
            ws = r"[^\S\r\n]"
            return f'{match[1]}{re.sub(ws, "", match[2])}{match[3]}{match[4]}'
        
        # Fix PART headers
        text = re.sub(
            r"(\n[^\S\r\n]*)(P[^\S\r\n]*A[^\S\r\n]*R[^\S\r\n]*T)([^\S\r\n]+)((\d{1,2}|[IV]{1,2})[AB]?)",
            remove_whitespace, text, flags=re.IGNORECASE
        )
        
        # Fix ITEM headers
        text = re.sub(
            r"(\n[^\S\r\n]*)(I[^\S\r\n]*T[^\S\r\n]*E[^\S\r\n]*M)([^\S\r\n]+)(\d{1,2}[AB]?)",
            remove_whitespace, text, flags=re.IGNORECASE
        )
        
        # Clean item formatting
        text = re.sub(
            r"(ITEM|PART)(\s+\d{1,2}[AB]?)([\-•])",
            r"\1\2 \3 ", text, flags=re.IGNORECASE
        )
        
        # Remove navigation elements
        navigation_patterns = [
            r"\n[^\S\r\n]*(TABLE\s+OF\s+CONTENTS|INDEX\s+TO\s+FINANCIAL\s+STATEMENTS|BACK\s+TO\s+CONTENTS|QUICKLINKS)[^\S\r\n]*\n",
            r"\n[^\S\r\n]*[-‒–—]*\d+[-‒–—]*[^\S\r\n]*\n",
            r"\n[^\S\r\n]*\d+[^\S\r\n]*\n",
            r"[\n\s]F[-‒–—]*\d+",
            r"\n[^\S\r\n]*Page\s[\d*]+[^\S\r\n]*\n"
        ]
        
        for pattern in navigation_patterns:
            text = re.sub(pattern, "\n", text, flags=self.regex_flags)
        
        return text
    
    def _remove_multiple_lines(self, text: str) -> str:
        """Remove excessive whitespace and newlines."""
        # Replace multiple newlines and spaces with temp token
        text = re.sub(r"(( )*\n( )*){2,}", "#NEWLINE", text)
        # Replace single newlines with spaces
        text = re.sub(r"\n", " ", text)
        # Replace temp token with single newline
        text = re.sub(r"(#NEWLINE)+", "\n", text).strip()
        # Replace multiple spaces with single space
        text = re.sub(r"[ ]{2,}", " ", text)
        
        return text
    
    def _parse_item_section(
        self,
        text: str,
        item_index: str,
        next_item_list: List[str],
        positions: List[int]
    ) -> Tuple[str, List[int]]:
        """Parse a specific item section from text."""
        # Prepare item index regex pattern
        item_pattern = self._prepare_item_pattern(item_index)
        
        possible_sections = []
        
        # Look for sections between current item and next items
        for next_item_index in next_item_list:
            if possible_sections:
                break
                
            next_item_pattern = self._prepare_item_pattern(next_item_index)
            
            # Find all matches for current item
            for match in re.finditer(
                rf"\n[^\S\r\n]*ITEM\s+{item_pattern}[.*~\-:\s]",
                text, flags=self.regex_flags
            ):
                offset = match.start()
                
                # Look for end pattern (next item)
                end_matches = list(re.finditer(
                    rf"\n[^\S\r\n]*ITEM\s+{item_pattern}[.*~\-:\s].+?([^\S\r\n]*ITEM\s+{next_item_pattern}[.*~\-:\s])",
                    text[offset:], flags=self.regex_flags
                ))
                
                if end_matches:
                    possible_sections.append((offset, end_matches))
        
        # Extract the best section
        section_text, positions = self._get_best_section(possible_sections, text, positions)
        
        # Handle last item case
        if not section_text and item_index in ["14", "15"]:
            section_text = self._get_last_item_section(item_index, text, positions)
        
        return section_text.strip(), positions
    
    def _prepare_item_pattern(self, item_index: str) -> str:
        """Prepare regex pattern for item index."""
        if item_index == "9A":
            return item_index.replace("A", r"[^\S\r\n]*A(?:\(T\))?")
        elif "A" in item_index:
            return item_index.replace("A", r"[^\S\r\n]*A")
        elif "B" in item_index:
            return item_index.replace("B", r"[^\S\r\n]*B")
        return item_index
    
    def _get_best_section(
        self,
        possible_sections: List[Tuple[int, List]],
        text: str,
        positions: List[int]
    ) -> Tuple[str, List[int]]:
        """Select the best section from possible matches."""
        max_length = 0
        best_match = None
        best_offset = None
        
        for offset, matches in possible_sections:
            for match in matches:
                length = match.end() - match.start()
                
                # Check if match is after previous positions
                if positions:
                    if length > max_length and offset + match.start() >= positions[-1]:
                        best_match = match
                        best_offset = offset
                        max_length = length
                elif length > max_length:
                    best_match = match
                    best_offset = offset
                    max_length = length
        
        if best_match and best_offset is not None:
            # Extract section text
            start_pos = best_offset + best_match.start()
            end_pos = best_offset + best_match.regs[1][0]
            
            section_text = text[start_pos:end_pos]
            positions.append(best_offset + best_match.end() - len(best_match[1]) - 1)
            
            return section_text, positions
        
        return "", positions
    
    def _get_last_item_section(self, item_index: str, text: str, positions: List[int]) -> str:
        """Extract the last item section that goes to end of document."""
        pattern = rf"\n[^\S\r\n]*ITEM\s+{item_index}[.\-:\s].+?"
        
        for match in re.finditer(pattern, text, flags=self.regex_flags):
            if positions and match.start() >= positions[-1]:
                return text[match.start():].strip()
            elif not positions:
                return text[match.start():].strip()
        
        return ""
    
    def _remove_html_tables(self, content: Union[BeautifulSoup, str], is_html: bool) -> Union[BeautifulSoup, str]:
        """Remove HTML tables containing primarily numerical data."""
        if not is_html:
            # For plain text, remove table tags
            return re.sub(r"<TABLE>.*?</TABLE>", "", str(content), flags=self.regex_flags)
        
        # For HTML content, analyze and remove numerical tables
        if isinstance(content, str):
            content = BeautifulSoup(content, 'html.parser')
        
        tables = content.find_all("table")
        
        for table in tables:
            if self._should_remove_table(table):
                table.decompose()
        
        return content
    
    def _should_remove_table(self, table) -> bool:
        """Determine if a table should be removed based on content analysis."""
        try:
            # Check for item headers in table
            table_text = self._clean_text(self._strip_html(str(table)))
            
            # Don't remove tables containing item headers
            for item_index in self.items_list:
                pattern = rf"\n[^\S\r\n]*ITEM\s+{item_index}[.*~\-:\s]"
                if re.search(pattern, table_text, flags=self.regex_flags):
                    return False
            
            # Check for background colors (indicates formatted table)
            elements_with_style = (
                table.find_all("tr", attrs={"style": True}) +
                table.find_all("td", attrs={"style": True}) +
                table.find_all("th", attrs={"style": True})
            )
            
            for element in elements_with_style:
                style = cssutils.parseStyle(element["style"])
                bg_color = style.get("background") or style.get("background-color")
                
                if bg_color and bg_color.lower() not in [
                    "none", "transparent", "#ffffff", "#fff", "white"
                ]:
                    return True
            
            # Check bgcolor attribute
            elements_with_bgcolor = (
                table.find_all("tr", attrs={"bgcolor": True}) +
                table.find_all("td", attrs={"bgcolor": True}) +
                table.find_all("th", attrs={"bgcolor": True})
            )
            
            for element in elements_with_bgcolor:
                bgcolor = element["bgcolor"].lower()
                if bgcolor not in ["none", "transparent", "#ffffff", "#fff", "white"]:
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error analyzing table: {e}")
            return False
    
    async def extract_filing_sections_async(
        self,
        filing_content: str,
        items_to_extract: Optional[List[str]] = None
    ) -> Dict[str, str]:
        """Async wrapper for section extraction."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.extract_sections_from_content,
            filing_content,
            items_to_extract
        )


# Convenience functions
def extract_10k_sections(
    html_content: str,
    items_to_extract: Optional[List[str]] = None,
    remove_tables: bool = False
) -> Dict[str, str]:
    """Extract 10-K sections from HTML content."""
    extractor = AdvancedTextExtractor(remove_tables=remove_tables)
    return extractor.extract_sections_from_content(html_content, items_to_extract)


async def extract_10k_sections_async(
    html_content: str,
    items_to_extract: Optional[List[str]] = None,
    remove_tables: bool = False
) -> Dict[str, str]:
    """Async version of 10-K section extraction."""
    extractor = AdvancedTextExtractor(remove_tables=remove_tables)
    return await extractor.extract_filing_sections_async(html_content, items_to_extract)