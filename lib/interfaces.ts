export interface IDataSource {
	connector?: any;
	settings: {
		connector: string;
		url: string;
		username: string;
		password: string;
		database: string;
		debug?: boolean;
	}
}

export type ICallback = (error: Error | null, result?: any) => void;
