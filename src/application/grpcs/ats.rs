use tracing::instrument;

pub mod decryptor {
    tonic::include_proto!("decryptor");
}

pub struct AtsDecryptorGrpc {
    // Cocok dengan `service Decryptor` di file .proto Anda
    client: decryptor::decryptor_service_client::DecryptorServiceClient<tonic::transport::Channel>,
}

impl AtsDecryptorGrpc {
    /// Membuat instance client gRPC baru
    pub fn new(host: &str) -> Self {
        let channel = tonic::transport::Endpoint::from_shared(host.to_string())
            .expect("invalid facilitator grpc url")
            .connect_lazy();

        Self {
            client: decryptor::decryptor_service_client::DecryptorServiceClient::new(channel),
        }
    }

    #[instrument(skip(self), fields(types = "ats decryptor "))]
    pub async fn decrypt(
        &self,
        request: decryptor::DecryptRequest,
    ) -> Result<decryptor::DecryptResponse, tonic::Status> {
        let mut client = self.client.clone();

        // 1. tonic::Request::new diperlukan untuk membungkus request
        // 2. .await mengembalikan Result<Response<DecryptResponse>, Status>
        // 3. .map(|r| r.into_inner()) digunakan untuk mengambil data asli dari dalam Response
        client
            .decrypt(tonic::Request::new(request))
            .await
            .map(|response| response.into_inner())
    }
}
