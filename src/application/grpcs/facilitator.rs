pub mod s3_proto {
    // String "s3" ini cocok dengan `package s3;` di file .proto Anda
    tonic::include_proto!("s3");
}

pub struct S3FacilitatorGrpc {
    // Cocok dengan `service S3Service` di file .proto Anda
    client: s3_proto::s3_service_client::S3ServiceClient<tonic::transport::Channel>,
}

impl S3FacilitatorGrpc {
    /// Membuat instance client gRPC baru
    pub fn new(host: &str) -> Self {
        let channel = tonic::transport::Endpoint::from_shared(host.to_string())
            .expect("invalid facilitator grpc url")
            .connect_lazy();

        Self {
            client: s3_proto::s3_service_client::S3ServiceClient::new(channel),
        }
    }

    /// Contoh cara memanggil rpc UploadFile
    pub async fn upload_file(
        &self,
        request: s3_proto::UploadFileRequest,
    ) -> Result<tonic::Response<s3_proto::UploadFileResponse>, tonic::Status> {
        let mut client = self.client.clone();

        // Memanggil method gRPC yang sudah digenerate otomatis
        client.upload_file(request).await
    }

    /// Contoh cara memanggil rpc GetDownloadUrl
    pub async fn get_download_url(
        &self,
        keys: Vec<String>,
    ) -> Result<tonic::Response<s3_proto::GetDownloadUrlResponse>, tonic::Status> {
        let mut client = self.client.clone();
        let request = s3_proto::GetDownloadUrlRequest { keys };
        client.get_download_url(request).await
    }
}
