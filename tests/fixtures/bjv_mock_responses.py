"""
Mock HTTP responses for BJV integration tests.
"""

MOCK_SEARCH_PAGE_1 = """
<html>
<body>
<div class="search-results">
  <div class="book-item" data-id="12345">
    <a href="/libros/libro.htm?l=12345">
      <h3>Derecho Civil Mexicano Tomo I</h3>
    </a>
    <span class="author">García, Roberto</span>
    <span class="year">2020</span>
  </div>
  <div class="book-item" data-id="12346">
    <a href="/libros/libro.htm?l=12346">
      <h3>Introducción al Derecho</h3>
    </a>
    <span class="author">López, María</span>
    <span class="year">2019</span>
  </div>
</div>
<div class="pagination">
  <a href="?page=2">Siguiente</a>
</div>
</body>
</html>
"""

MOCK_SEARCH_PAGE_2 = """
<html>
<body>
<div class="search-results">
  <div class="book-item" data-id="12347">
    <a href="/libros/libro.htm?l=12347">
      <h3>Derecho Penal Mexicano</h3>
    </a>
    <span class="author">Martínez, Juan</span>
    <span class="year">2021</span>
  </div>
</div>
</body>
</html>
"""

MOCK_BOOK_DETAIL = """
<html>
<body>
<div class="book-detail">
  <h1>Derecho Civil Mexicano Tomo I</h1>
  <div class="metadata">
    <span class="author">García, Roberto</span>
    <span class="publisher">UNAM</span>
    <span class="year">2020</span>
    <span class="isbn">978-607-30-1234-5</span>
    <span class="pages">450</span>
  </div>
  <div class="chapters">
    <div class="chapter" data-id="cap1">
      <a href="/pdf/12345/cap1.pdf">Capítulo 1: Introducción</a>
    </div>
    <div class="chapter" data-id="cap2">
      <a href="/pdf/12345/cap2.pdf">Capítulo 2: Personas</a>
    </div>
  </div>
</div>
</body>
</html>
"""

MOCK_BOOK_DETAIL_MINIMAL = """
<html>
<body>
<div class="book-detail">
  <h1>Libro de Prueba</h1>
  <div class="metadata">
    <span class="author">Autor Test</span>
  </div>
</div>
</body>
</html>
"""

# Minimal PDF for testing (valid PDF structure)
MOCK_PDF_BYTES = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]
   /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length 44 >>
stream
BT /F1 12 Tf 100 700 Td (Test PDF Content) Tj ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
xref
0 6
trailer
<< /Size 6 /Root 1 0 R >>
startxref
%%EOF
"""
