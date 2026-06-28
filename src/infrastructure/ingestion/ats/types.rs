pub enum AtsIngestionTable {
    Candidate,
    Experience,
    Education,
    Vacancy,
    ApplicantBucket,
    CandidateClientField,
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
        }
    }
}
