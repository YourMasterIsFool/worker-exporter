pub enum AtsIngestionTable {
    Candidate,
    Experience,
    Education,
    Vacancy,
    VacancyCustomField,
    ApplicantBucket,
    ApplicantSnapshot,
    CandidateClientField,
    RequirementStageTemplate,
    RequirementStageTemplateDetail,
    FormField,
    FormTemplate,
    FormTemplateDetail,
}

impl AtsIngestionTable {
    pub fn table_name(&self) -> &str {
        match self {
            AtsIngestionTable::Candidate => "candidate.candidate",
            AtsIngestionTable::Experience => "candidate.experient",
            AtsIngestionTable::Education => "candidate.education",
            AtsIngestionTable::Vacancy => "vacancy.vacancy",
            AtsIngestionTable::VacancyCustomField => "vacancy.vacancy_custom_field",
            AtsIngestionTable::ApplicantBucket => "applicant.applicant_bucket",
            AtsIngestionTable::ApplicantSnapshot => "applicant.applicant_snapshots",
            AtsIngestionTable::CandidateClientField => "candidate.candidate_client_field",
            AtsIngestionTable::RequirementStageTemplate => "applicant.recruitment_stage_template",
            AtsIngestionTable::RequirementStageTemplateDetail => {
                "applicant.recruitment_stage_template_detail"
            }
            AtsIngestionTable::FormField => "form.field",
            AtsIngestionTable::FormTemplate => "form.template",
            AtsIngestionTable::FormTemplateDetail => "form.template_detail",
        }
    }
}
