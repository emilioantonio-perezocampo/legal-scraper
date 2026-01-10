"""
RED Phase Tests: SCJN Value Objects

Tests for SCJN-specific enumerations and value objects.
These tests must FAIL initially (RED phase).
"""
import pytest
from datetime import date

# These imports will fail until implementation exists
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
    SubjectMatter,
)


class TestDocumentCategory:
    """Tests for DocumentCategory enumeration."""

    def test_category_constitucion_exists(self):
        """CONSTITUCION category should exist."""
        assert DocumentCategory.CONSTITUCION.value == "CONSTITUCION"

    def test_category_ley_exists(self):
        """LEY category should exist."""
        assert DocumentCategory.LEY.value == "LEY"

    def test_category_ley_federal_exists(self):
        """LEY_FEDERAL category should exist."""
        assert DocumentCategory.LEY_FEDERAL.value == "LEY_FEDERAL"

    def test_category_ley_general_exists(self):
        """LEY_GENERAL category should exist."""
        assert DocumentCategory.LEY_GENERAL.value == "LEY_GENERAL"

    def test_category_ley_organica_exists(self):
        """LEY_ORGANICA category should exist."""
        assert DocumentCategory.LEY_ORGANICA.value == "LEY_ORGANICA"

    def test_category_codigo_exists(self):
        """CODIGO category should exist."""
        assert DocumentCategory.CODIGO.value == "CODIGO"

    def test_category_decreto_exists(self):
        """DECRETO category should exist."""
        assert DocumentCategory.DECRETO.value == "DECRETO"

    def test_category_reglamento_exists(self):
        """REGLAMENTO category should exist."""
        assert DocumentCategory.REGLAMENTO.value == "REGLAMENTO"

    def test_category_acuerdo_exists(self):
        """ACUERDO category should exist."""
        assert DocumentCategory.ACUERDO.value == "ACUERDO"

    def test_category_tratado_exists(self):
        """TRATADO category should exist."""
        assert DocumentCategory.TRATADO.value == "TRATADO"

    def test_category_convenio_exists(self):
        """CONVENIO category should exist."""
        assert DocumentCategory.CONVENIO.value == "CONVENIO"

    def test_all_categories_are_unique(self):
        """All category values should be unique."""
        values = [cat.value for cat in DocumentCategory]
        assert len(values) == len(set(values))

    def test_category_count(self):
        """Should have at least 11 categories."""
        assert len(DocumentCategory) >= 11


class TestDocumentScope:
    """Tests for DocumentScope enumeration."""

    def test_scope_federal_exists(self):
        """FEDERAL scope should exist."""
        assert DocumentScope.FEDERAL.value == "FEDERAL"

    def test_scope_estatal_exists(self):
        """ESTATAL scope should exist."""
        assert DocumentScope.ESTATAL.value == "ESTATAL"

    def test_scope_cdmx_exists(self):
        """CDMX scope should exist."""
        assert DocumentScope.CDMX.value == "CDMX"

    def test_scope_internacional_exists(self):
        """INTERNACIONAL scope should exist."""
        assert DocumentScope.INTERNACIONAL.value == "INTERNACIONAL"

    def test_scope_extranjera_exists(self):
        """EXTRANJERA scope should exist."""
        assert DocumentScope.EXTRANJERA.value == "EXTRANJERA"

    def test_all_scopes_are_unique(self):
        """All scope values should be unique."""
        values = [scope.value for scope in DocumentScope]
        assert len(values) == len(set(values))


class TestDocumentStatus:
    """Tests for DocumentStatus enumeration."""

    def test_status_vigente_exists(self):
        """VIGENTE status should exist."""
        assert DocumentStatus.VIGENTE.value == "VIGENTE"

    def test_status_abrogada_exists(self):
        """ABROGADA status should exist."""
        assert DocumentStatus.ABROGADA.value == "ABROGADA"

    def test_status_derogada_exists(self):
        """DEROGADA status should exist."""
        assert DocumentStatus.DEROGADA.value == "DEROGADA"

    def test_status_sustituida_exists(self):
        """SUSTITUIDA status should exist."""
        assert DocumentStatus.SUSTITUIDA.value == "SUSTITUIDA"

    def test_status_extinta_exists(self):
        """EXTINTA status should exist."""
        assert DocumentStatus.EXTINTA.value == "EXTINTA"

    def test_all_statuses_are_unique(self):
        """All status values should be unique."""
        values = [status.value for status in DocumentStatus]
        assert len(values) == len(set(values))


class TestSubjectMatter:
    """Tests for SubjectMatter enumeration."""

    def test_subject_administrativo_exists(self):
        """ADMINISTRATIVO subject should exist."""
        assert SubjectMatter.ADMINISTRATIVO.value == "ADMINISTRATIVO"

    def test_subject_civil_exists(self):
        """CIVIL subject should exist."""
        assert SubjectMatter.CIVIL.value == "CIVIL"

    def test_subject_constitucional_exists(self):
        """CONSTITUCIONAL subject should exist."""
        assert SubjectMatter.CONSTITUCIONAL.value == "CONSTITUCIONAL"

    def test_subject_electoral_exists(self):
        """ELECTORAL subject should exist."""
        assert SubjectMatter.ELECTORAL.value == "ELECTORAL"

    def test_subject_familiar_exists(self):
        """FAMILIAR subject should exist."""
        assert SubjectMatter.FAMILIAR.value == "FAMILIAR"

    def test_subject_fiscal_exists(self):
        """FISCAL subject should exist."""
        assert SubjectMatter.FISCAL.value == "FISCAL"

    def test_subject_laboral_exists(self):
        """LABORAL subject should exist."""
        assert SubjectMatter.LABORAL.value == "LABORAL"

    def test_subject_mercantil_exists(self):
        """MERCANTIL subject should exist."""
        assert SubjectMatter.MERCANTIL.value == "MERCANTIL"

    def test_subject_penal_exists(self):
        """PENAL subject should exist."""
        assert SubjectMatter.PENAL.value == "PENAL"

    def test_subject_procesal_exists(self):
        """PROCESAL subject should exist."""
        assert SubjectMatter.PROCESAL.value == "PROCESAL"

    def test_all_subjects_are_unique(self):
        """All subject values should be unique."""
        values = [subj.value for subj in SubjectMatter]
        assert len(values) == len(set(values))

    def test_subject_count(self):
        """Should have at least 10 subjects."""
        assert len(SubjectMatter) >= 10
