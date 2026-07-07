pub enum AtsIngestionTable {
    Candidate,
    Experience,
    Education,
    Vacancy,
    ApplicantBucket,
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
            AtsIngestionTable::ApplicantBucket => "applicant.applicant_bucket",
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
