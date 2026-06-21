# ITK overview deck

An emailable one-page-per-section overview of ITK, styled in the dashboard's
**Flexoki Light** theme (paper background `#FFFCF0`, Flexoki accent colours,
IBM Plex Sans / Mono typography — see
`src/aemo_dashboard/shared/flexoki_theme.py`).

## Files

| File | Purpose |
|------|---------|
| `itk_overview.md`   | Source content (edit this) |
| `build_deck.py`     | Generates the PowerPoint and PDF |
| `itk_overview.pptx` | PowerPoint deck (editable) |
| `itk_overview.pdf`  | PDF (ready to email) |

## Regenerating

```bash
python build_deck.py
```

### Dependencies

- `python-pptx`  — `pip install python-pptx`
- LibreOffice with Impress — `apt-get install libreoffice-impress`
  (used headless to convert the `.pptx` to `.pdf`)
- IBM Plex fonts — `apt-get install fonts-ibm-plex`
  (so the PDF renders in the Flexoki typeface)

Edit `itk_overview.md` (one `##` heading per slide, `-` for bullets) and rerun
`build_deck.py` to refresh both outputs.
