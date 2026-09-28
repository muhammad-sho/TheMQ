import type {
  IAuthenticateGeneric,
  ICredentialTestRequest,
  ICredentialType,
  INodeProperties,
} from "n8n-workflow";

export class TheMqApi implements ICredentialType {
  name = "theMqApi";

  displayName = "TheMQ API";

  documentationUrl = "https://github.com/muhammad-sho/TheMQ/tree/main/n8n-nodes-themq";

  properties: INodeProperties[] = [
    {
      displayName: "Base URL",
      name: "baseUrl",
      type: "string",
      default: "http://localhost:3000",
      placeholder: "http://themq:3000",
      description: "Base URL of TheMQ HTTP API (no trailing slash)",
      required: true,
    },
    {
      displayName: "API Token",
      name: "apiToken",
      type: "string",
      typeOptions: { password: true },
      default: "",
      description:
        "Bearer token: the API_TOKEN value pinned in docker-compose.yml (or your override)",
      required: true,
    },
  ];

  authenticate: IAuthenticateGeneric = {
    type: "generic",
    properties: {
      headers: {
        Authorization: "=Bearer {{$credentials.apiToken}}",
      },
    },
  };

  test: ICredentialTestRequest = {
    request: {
      baseURL: "={{$credentials.baseUrl}}",
      url: "/queues",
      method: "GET",
    },
  };
}
