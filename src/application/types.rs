pub enum EmailProvider {
    SendGrid,
}

impl EmailProvider {
    pub fn to_string(&self) -> String {
        match self {
            EmailProvider::SendGrid => "SENDGRID".into(),
        }
    }
}
