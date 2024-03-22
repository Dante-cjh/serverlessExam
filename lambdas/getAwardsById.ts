import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { Movie, MovieCast, MovieAward } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    QueryCommand,
    QueryCommandInput,
    GetCommand,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

type ResponseBody = {
    data: {
        movieAward: MovieAward
    };
};
// Enable coercion so that the string 'true' is coerced to
// boolean true before validation is performed.
const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
    schema.definitions["AwardDetailQuery"] || {}
);
const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {
        // Print Event
        console.log("Event: ", JSON.stringify(event));
        const parameters = event?.pathParameters;
        const movieId = parameters?.movieId
            ? parseInt(parameters.movieId)
            : undefined;

        const awardBody = parameters?.awardBody;

        const minReward = event.queryStringParameters?.min;

        if (!movieId) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Missing movie Id" }),
            };
        }

        if (!awardBody) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Missing award body" }),
            };
        }

        const keyConditionExpression: string = "movieId = :movieId AND awardBody = :awardBody";
        let expressionAttributeValues: any = {
            ":movieId": movieId,
            ":awardBody": awardBody
        };

        let filterExpression = '';

        if (minReward) {
            filterExpression += 'numAwards >= :minReward';
            expressionAttributeValues[":minReward"] = parseInt(minReward);
        }

        const queryCommandInput: any = {
            TableName: process.env.TABLE_NAME, // 确保环境变量名称正确
            KeyConditionExpression: keyConditionExpression,
            ExpressionAttributeValues: expressionAttributeValues,
        };

        if (filterExpression) {
            queryCommandInput["FilterExpression"] = filterExpression;
        }

        const queryOutput = await ddbDocClient.send(new QueryCommand(queryCommandInput));
        console.log("QueryCommand response: ", queryOutput);

        // @ts-ignore
        if (queryOutput.Items.length === 0){
            return {
                statusCode: 400,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Request failed" }),
            };
        }

        // Return Response
        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify(queryOutput.Items),
        };
    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ error }),
        };
    }
};

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
