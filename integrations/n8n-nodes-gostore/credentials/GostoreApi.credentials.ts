import type {
	ICredentialType,
	INodeProperties,
	ICredentialTestRequest,
} from 'n8n-workflow';

export class GostoreApi implements ICredentialType {
	name = 'gostoreApi';

	displayName = 'gostore API';

	documentationUrl = 'https://github.com/fdsriopreto-code/gostore';

	properties: INodeProperties[] = [
		{
			displayName: 'Endpoint',
			name: 'endpoint',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'https://storage.example.com',
			description:
				'Base URL of the gostore server. No trailing slash and no /gostore suffix — the S3 API is served at the root.',
		},
		{
			displayName: 'Region',
			name: 'region',
			type: 'string',
			default: 'us-east-1',
			description: 'Region string used for SigV4 signing. gostore defaults to us-east-1.',
		},
		{
			displayName: 'Access Key ID',
			name: 'accessKeyId',
			type: 'string',
			default: '',
			required: true,
		},
		{
			displayName: 'Secret Access Key',
			name: 'secretAccessKey',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			required: true,
		},
	];

	// Health endpoint is unauthenticated — a 200 means the endpoint URL is right.
	test: ICredentialTestRequest = {
		request: {
			baseURL: '={{$credentials.endpoint}}',
			url: '/gostore/health/live',
			method: 'GET',
		},
	};
}
