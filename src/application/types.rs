pub enum EmailProvider {
    SendGrid,
}

#[derive(Default, Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthContextPayload {
    pub actor_id: Option<String>,
    pub tenant_id: Option<String>,
    pub group_id: Option<String>,
}

impl EmailProvider {
    pub fn to_string(&self) -> String {
        match self {
            EmailProvider::SendGrid => "SENDGRID".into(),
        }
    }
}
