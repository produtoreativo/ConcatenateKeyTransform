package br.com.produtoreativo.kafka.transform;

public class App 
{
    public static void main( String[] args )
    {
        String [] keyFieldNames = "CodigoCliente,CodAgrupamento,CodigoProduto,SequenciaOrdenacaoProduto".split(",");
        for (String fieldName : keyFieldNames) {
         System.out.println( fieldName );
        }
        // new ConcatenateKeyTransform();
    }
}
