// Code generated by smithy-go-codegen DO NOT EDIT.

package dynamodb

import (
	"context"
	"fmt"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// This operation allows you to perform batch reads or writes on data stored in
// DynamoDB, using PartiQL. Each read statement in a BatchExecuteStatement must
// specify an equality condition on all key attributes. This enforces that each
// SELECT statement in a batch returns at most a single item. For more information,
// see [Running batch operations with PartiQL for DynamoDB].
//
// The entire batch must consist of either read statements or write statements,
// you cannot mix both in one batch.
//
// A HTTP 200 response does not mean that all statements in the
// BatchExecuteStatement succeeded. Error details for individual statements can be
// found under the [Error]field of the BatchStatementResponse for each statement.
//
// [Error]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchStatementResponse.html#DDB-Type-BatchStatementResponse-Error
// [Running batch operations with PartiQL for DynamoDB]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.multiplestatements.batching.html
func (c *Client) BatchExecuteStatement(ctx context.Context, params *BatchExecuteStatementInput, optFns ...func(*Options)) (*BatchExecuteStatementOutput, error) {
	if params == nil {
		params = &BatchExecuteStatementInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "BatchExecuteStatement", params, optFns, c.addOperationBatchExecuteStatementMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*BatchExecuteStatementOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type BatchExecuteStatementInput struct {

	// The list of PartiQL statements representing the batch to run.
	//
	// This member is required.
	Statements []types.BatchStatementRequest

	// Determines the level of detail about either provisioned or on-demand throughput
	// consumption that is returned in the response:
	//
	//   - INDEXES - The response includes the aggregate ConsumedCapacity for the
	//   operation, together with ConsumedCapacity for each table and secondary index
	//   that was accessed.
	//
	// Note that some operations, such as GetItem and BatchGetItem , do not access any
	//   indexes at all. In these cases, specifying INDEXES will only return
	//   ConsumedCapacity information for table(s).
	//
	//   - TOTAL - The response includes only the aggregate ConsumedCapacity for the
	//   operation.
	//
	//   - NONE - No ConsumedCapacity details are included in the response.
	ReturnConsumedCapacity types.ReturnConsumedCapacity

	noSmithyDocumentSerde
}

type BatchExecuteStatementOutput struct {

	// The capacity units consumed by the entire operation. The values of the list are
	// ordered according to the ordering of the statements.
	ConsumedCapacity []types.ConsumedCapacity

	// The response to each PartiQL statement in the batch. The values of the list are
	// ordered according to the ordering of the request statements.
	Responses []types.BatchStatementResponse

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationBatchExecuteStatementMiddlewares(stack *middleware.Stack, options Options) (err error) {
	if err := stack.Serialize.Add(&setOperationInputMiddleware{}, middleware.After); err != nil {
		return err
	}
	err = stack.Serialize.Add(&awsAwsjson10_serializeOpBatchExecuteStatement{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsAwsjson10_deserializeOpBatchExecuteStatement{}, middleware.After)
	if err != nil {
		return err
	}
	if err := addProtocolFinalizerMiddlewares(stack, options, "BatchExecuteStatement"); err != nil {
		return fmt.Errorf("add protocol finalizers: %v", err)
	}

	if err = addlegacyEndpointContextSetter(stack, options); err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = addClientRequestID(stack); err != nil {
		return err
	}
	if err = addComputeContentLength(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = addComputePayloadSHA256(stack); err != nil {
		return err
	}
	if err = addRetry(stack, options); err != nil {
		return err
	}
	if err = addRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = addRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addClientUserAgent(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addSetLegacyContextSigningOptionsMiddleware(stack); err != nil {
		return err
	}
	if err = addTimeOffsetBuild(stack, c); err != nil {
		return err
	}
	if err = addUserAgentRetryMode(stack, options); err != nil {
		return err
	}
	if err = addOpBatchExecuteStatementValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opBatchExecuteStatement(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addRecursionDetection(stack); err != nil {
		return err
	}
	if err = addRequestIDRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = addValidateResponseChecksum(stack, options); err != nil {
		return err
	}
	if err = addAcceptEncodingGzip(stack, options); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = addDisableHTTPSMiddleware(stack, options); err != nil {
		return err
	}
	return nil
}

func newServiceMetadataMiddleware_opBatchExecuteStatement(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		OperationName: "BatchExecuteStatement",
	}
}