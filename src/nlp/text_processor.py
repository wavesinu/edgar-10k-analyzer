"""EDGAR 10-K 정성적 섹션을 위한 고급 텍스트 처리."""

import re
import asyncio
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from bs4 import BeautifulSoup, NavigableString, Tag
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from collections import Counter
import string
from loguru import logger

from src.database.schema import QualitativeSection
from src.database.connection import db_client


@dataclass
class SectionMetadata:
    """Metadata for a extracted section."""
    name: str
    title: str
    content: str
    word_count: int
    char_count: int
    sentences: List[str]
    key_phrases: List[str]


class TextProcessor:
    """Advanced text processor for 10-K qualitative analysis."""
    
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words('english'))
        
        # Download required NLTK data
        self._ensure_nltk_data()
        
        # Section identification patterns
        self.section_patterns = {
            "business": {
                "patterns": [
                    r"item\s+1\s*[.\-–—]\s*business",
                    r"item\s+1\b(?!\s*a).*?business",
                    r"business\s+(?:overview|description|operations)",
                    r"our\s+business",
                    r"company\s+overview"
                ],
                "titles": ["Item 1. Business", "Business", "Business Overview", "Our Business"]
            },
            "risk_factors": {
                "patterns": [
                    r"item\s+1a\s*[.\-–—]\s*risk\s+factors",
                    r"item\s+1a\b.*?risk\s+factors",
                    r"risk\s+factors",
                    r"risks?\s+(?:related\s+to|associated\s+with)",
                    r"principal\s+risks?"
                ],
                "titles": ["Item 1A. Risk Factors", "Risk Factors", "Principal Risks"]
            },
            "mda": {
                "patterns": [
                    r"item\s+7\s*[.\-–—]\s*management[''']?s\s+discussion\s+and\s+analysis",
                    r"item\s+7\b.*?management.*?discussion.*?analysis",
                    r"management[''']?s\s+discussion\s+and\s+analysis",
                    r"md&a",
                    r"financial\s+condition\s+and\s+results\s+of\s+operations"
                ],
                "titles": ["Item 7. Management's Discussion and Analysis", "MD&A", 
                          "Management's Discussion and Analysis of Financial Condition and Results of Operations"]
            },
            "financial_statements": {
                "patterns": [
                    r"item\s+8\s*[.\-–—]\s*financial\s+statements",
                    r"item\s+8\b.*?financial\s+statements",
                    r"consolidated\s+(?:balance\s+sheets?|statements?)",
                    r"financial\s+statements\s+and\s+supplementary\s+data"
                ],
                "titles": ["Item 8. Financial Statements and Supplementary Data", 
                          "Financial Statements", "Consolidated Financial Statements"]
            }
        }
        
        # Key business terms for analysis
        self.business_terms = {
            "growth": ["growth", "expand", "expansion", "increase", "growing", "develop", "scale", "penetrate"],
            "innovation": ["innovation", "technology", "digital", "automation", "ai", "artificial intelligence", 
                          "machine learning", "research", "development", "r&d", "patent"],
            "competition": ["competition", "competitive", "competitor", "market share", "differentiation", 
                           "advantage", "moat", "barrier", "rivalry"],
            "financial_health": ["revenue", "profit", "margin", "cash", "debt", "leverage", "liquidity", 
                                "working capital", "free cash flow"],
            "risk_indicators": ["uncertainty", "volatile", "decline", "loss", "challenge", "pressure", 
                               "adverse", "negative", "concern", "threat"],
            "opportunity": ["opportunity", "potential", "favorable", "benefit", "advantage", "positive", 
                           "strong", "robust", "momentum"]
        }
    
    def _ensure_nltk_data(self):
        """Download required NLTK data if not present."""
        try:
            nltk.data.find('tokenizers/punkt')
            nltk.data.find('corpora/stopwords')
            nltk.data.find('corpora/wordnet')
            nltk.data.find('taggers/averaged_perceptron_tagger')
        except LookupError:
            logger.info("Downloading required NLTK data...")
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            nltk.download('wordnet', quiet=True)
            nltk.download('averaged_perceptron_tagger', quiet=True)
    
    def clean_html_content(self, html_content: str) -> str:
        """Clean and extract text from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text and clean it
        text = soup.get_text()
        
        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)  # Multiple newlines to double newline
        text = re.sub(r'[ \t]+', ' ', text)      # Multiple spaces to single space
        text = text.strip()
        
        return text
    
    def identify_section_boundaries(self, text: str) -> Dict[str, Tuple[int, int]]:
        """Identify start and end positions of each section in the text."""
        text_lower = text.lower()
        boundaries = {}
        
        # Find all section starts
        section_positions = []
        
        for section_name, config in self.section_patterns.items():
            for pattern in config["patterns"]:
                matches = list(re.finditer(pattern, text_lower, re.IGNORECASE | re.MULTILINE))
                for match in matches:
                    section_positions.append((match.start(), section_name, match.group()))
        
        # Sort by position
        section_positions.sort(key=lambda x: x[0])
        
        # Determine boundaries
        for i, (start_pos, section_name, match_text) in enumerate(section_positions):
            # Find end position (start of next section or end of document)
            if i + 1 < len(section_positions):
                end_pos = section_positions[i + 1][0]
            else:
                end_pos = len(text)
            
            # Only keep if we haven't seen this section or this one is longer
            if section_name not in boundaries or (end_pos - start_pos) > (boundaries[section_name][1] - boundaries[section_name][0]):
                boundaries[section_name] = (start_pos, end_pos)
        
        return boundaries
    
    def extract_sections(self, html_content: str) -> Dict[str, SectionMetadata]:
        """Extract and process qualitative sections from 10-K HTML."""
        logger.info("Extracting qualitative sections from 10-K filing")
        
        # Clean HTML content
        text = self.clean_html_content(html_content)
        
        # Identify section boundaries
        boundaries = self.identify_section_boundaries(text)
        
        sections = {}
        
        for section_name, (start_pos, end_pos) in boundaries.items():
            try:
                # Extract section content
                content = text[start_pos:end_pos].strip()
                
                # Skip if section is too short
                if len(content) < 500:
                    logger.warning(f"Section '{section_name}' too short ({len(content)} chars), skipping")
                    continue
                
                # Determine section title
                title = self._determine_section_title(content, section_name)
                
                # Process content
                processed_content = self._clean_section_content(content)
                
                # Calculate metrics
                word_count = len(processed_content.split())
                char_count = len(processed_content)
                sentences = sent_tokenize(processed_content)
                key_phrases = self._extract_key_phrases(processed_content, section_name)
                
                sections[section_name] = SectionMetadata(
                    name=section_name,
                    title=title,
                    content=processed_content,
                    word_count=word_count,
                    char_count=char_count,
                    sentences=sentences,
                    key_phrases=key_phrases
                )
                
                logger.info(f"Extracted {section_name}: {word_count} words, {len(sentences)} sentences")
                
            except Exception as e:
                logger.error(f"Error processing section '{section_name}': {e}")
                continue
        
        return sections
    
    def _determine_section_title(self, content: str, section_name: str) -> str:
        """Determine the appropriate title for a section."""
        # Look for title in the first few lines
        first_lines = content[:500].split('\n')[:5]
        
        possible_titles = self.section_patterns[section_name]["titles"]
        
        for line in first_lines:
            line_clean = line.strip()
            for title in possible_titles:
                if title.lower() in line_clean.lower() and len(line_clean) < 100:
                    return line_clean
        
        # Default to first possible title
        return possible_titles[0]
    
    def _clean_section_content(self, content: str) -> str:
        """Clean and normalize section content."""
        # Remove excessive whitespace
        content = re.sub(r'\n\s*\n', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        
        # Remove page numbers and headers/footers
        content = re.sub(r'\n\s*\d+\s*\n', '\n', content)
        content = re.sub(r'\n\s*page\s+\d+.*?\n', '\n', content, flags=re.IGNORECASE)
        
        # Remove table of contents references
        content = re.sub(r'\.{3,}\s*\d+', '', content)
        
        # Clean up common artifacts
        content = re.sub(r'\s+', ' ', content)  # Multiple spaces to single
        content = content.strip()
        
        return content
    
    def _extract_key_phrases(self, content: str, section_name: str) -> List[str]:
        """Extract key phrases relevant to the section."""
        # Tokenize and clean
        words = word_tokenize(content.lower())
        words = [w for w in words if w.isalpha() and w not in self.stop_words and len(w) > 2]
        
        # Find multi-word phrases
        phrases = []
        
        # Look for business-relevant terms
        for category, terms in self.business_terms.items():
            for term in terms:
                term_pattern = r'\b' + re.escape(term) + r'\b'
                matches = len(re.findall(term_pattern, content.lower()))
                if matches > 0:
                    phrases.append(f"{term} ({matches})")
        
        # Extract most common words (excluding stop words)
        word_freq = Counter(words)
        common_words = [word for word, count in word_freq.most_common(20) if count > 2]
        
        return phrases + common_words
    
    async def process_and_store_sections(self, filing_id: str, html_content: str) -> List[str]:
        """Process sections and store them in the database."""
        logger.info(f"Processing and storing sections for filing {filing_id}")
        
        sections = self.extract_sections(html_content)
        stored_section_ids = []
        
        for section_name, metadata in sections.items():
            try:
                # Create qualitative section record
                section = QualitativeSection(
                    filing_id=filing_id,
                    section_name=section_name,
                    section_title=metadata.title,
                    content=metadata.content,
                    word_count=metadata.word_count,
                    char_count=metadata.char_count
                )
                
                # Store in database
                section_record = await db_client.insert_qualitative_section(section)
                section_id = section_record.get("id")
                
                if section_id:
                    stored_section_ids.append(section_id)
                    logger.info(f"Stored section '{section_name}' with ID {section_id}")
                
            except Exception as e:
                logger.error(f"Error storing section '{section_name}': {e}")
                continue
        
        logger.info(f"Successfully stored {len(stored_section_ids)} sections for filing {filing_id}")
        return stored_section_ids
    
    def analyze_section_quality(self, metadata: SectionMetadata) -> Dict[str, float]:
        """Analyze the quality and completeness of a section."""
        content = metadata.content.lower()
        
        quality_metrics = {
            "completeness": 0.0,      # 0-1 based on length and structure
            "informativeness": 0.0,   # 0-1 based on business terms density
            "readability": 0.0,       # 0-1 based on sentence structure
            "specificity": 0.0        # 0-1 based on specific vs generic content
        }
        
        # Completeness (based on word count)
        word_count = metadata.word_count
        if word_count > 2000:
            quality_metrics["completeness"] = 1.0
        elif word_count > 1000:
            quality_metrics["completeness"] = 0.8
        elif word_count > 500:
            quality_metrics["completeness"] = 0.6
        else:
            quality_metrics["completeness"] = 0.3
        
        # Informativeness (business terms density)
        business_term_count = 0
        total_terms = len(self.business_terms["growth"] + self.business_terms["innovation"] + 
                         self.business_terms["competition"] + self.business_terms["financial_health"])
        
        for category, terms in self.business_terms.items():
            for term in terms:
                if term in content:
                    business_term_count += 1
        
        quality_metrics["informativeness"] = min(1.0, business_term_count / (total_terms * 0.1))
        
        # Readability (sentence length variety)
        sentences = metadata.sentences
        if sentences:
            avg_sentence_length = sum(len(s.split()) for s in sentences) / len(sentences)
            if 15 <= avg_sentence_length <= 25:
                quality_metrics["readability"] = 1.0
            else:
                quality_metrics["readability"] = max(0.3, 1.0 - abs(avg_sentence_length - 20) / 20)
        
        # Specificity (presence of numbers, dates, specific terms)
        specificity_indicators = [
            r'\b\d{4}\b',              # Years
            r'\$\d+',                  # Dollar amounts
            r'\d+%',                   # Percentages
            r'\b(?:million|billion)\b', # Large numbers
            r'\b(?:quarter|q\d)\b',    # Time periods
        ]
        
        specificity_count = 0
        for pattern in specificity_indicators:
            matches = len(re.findall(pattern, content, re.IGNORECASE))
            specificity_count += min(matches, 5)  # Cap per indicator
        
        quality_metrics["specificity"] = min(1.0, specificity_count / 10)
        
        return quality_metrics


async def main():
    """Test text processing functionality."""
    # This would be used with actual HTML content from a 10-K filing
    processor = TextProcessor()
    
    # Example usage (would use real HTML content)
    sample_html = """
    <html>
    <body>
    <h1>Item 1. Business</h1>
    <p>Apple Inc. designs, manufactures and markets smartphones, personal computers, 
    tablets, wearables and accessories, and sells a variety of related services...</p>
    
    <h1>Item 1A. Risk Factors</h1>
    <p>The Company's business, reputation, results of operations, financial condition and 
    stock price can be affected by a number of factors, whether currently known or unknown...</p>
    </body>
    </html>
    """
    
    sections = processor.extract_sections(sample_html)
    
    for name, metadata in sections.items():
        quality = processor.analyze_section_quality(metadata)
        print(f"\n{name.upper()}:")
        print(f"  Words: {metadata.word_count}")
        print(f"  Quality metrics: {quality}")
        print(f"  Key phrases: {metadata.key_phrases[:5]}")


if __name__ == "__main__":
    asyncio.run(main())