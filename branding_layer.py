from dataclasses import dataclass
from html import escape
from typing import Optional


@dataclass
class BrandingProfile:
    advisor_name: str = ""
    firm_name: str = ""
    sender_display_name: str = ""
    brand_primary_color: str = "#0B1F3A"
    brand_secondary_color: str = "#F4F6F8"
    logo_url: str = ""
    website_url: str = ""
    phone: str = ""
    email_signature: str = ""
    custom_disclaimer: str = ""
    subject_prefix: str = ""
    cta_text: str = ""

    @property
    def resolved_sender_name(self) -> str:
        if self.sender_display_name:
            return self.sender_display_name
        if self.firm_name:
            return self.firm_name
        return "What You Need To Know About the Markets"

    @property
    def resolved_subject_prefix(self) -> str:
        if self.subject_prefix:
            return self.subject_prefix.strip()
        if self.firm_name:
            return self.firm_name.strip()
        return ""

    @property
    def resolved_cta_text(self) -> str:
        if self.cta_text:
            return self.cta_text
        return "Questions? Reply to this email."



def safe_color(value: str, fallback: str) -> str:
    value = (value or "").strip()
    if len(value) in {4, 7} and value.startswith("#"):
        return value
    return fallback



def build_signature_block(profile: BrandingProfile) -> str:
    lines = []
    if profile.email_signature:
        lines.append(escape(profile.email_signature).replace("\n", "<br>"))
    else:
        if profile.advisor_name:
            lines.append(f"<strong>{escape(profile.advisor_name)}</strong>")
        if profile.firm_name:
            lines.append(escape(profile.firm_name))
    contact_bits = []
    if profile.website_url:
        url = escape(profile.website_url)
        contact_bits.append(f"<a href=\"{url}\">Website</a>")
    if profile.phone:
        contact_bits.append(escape(profile.phone))
    if contact_bits:
        lines.append(" | ".join(contact_bits))
    return "<br>".join(lines)
