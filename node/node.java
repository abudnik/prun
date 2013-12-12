import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject.Kind;
import javax.tools.ForwardingJavaFileManager;

import java.security.SecureClassLoader;
import javax.tools.JavaFileManager;
import javax.tools.StandardJavaFileManager;
import javax.tools.FileObject;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;


public class node {
    private final static int NODE_SCRIPT_EXEC_FAILED = -5;

    public static void main(String args[]) {
        int errCode = 0;
        try {
            String readFifo = args[1];
            long scriptLen = Long.parseLong( args[2] );

            FileInputStream fifo = new FileInputStream( readFifo );
			byte[] bytes = new byte[ (int)scriptLen ];
            long bytesReaded = 0;
            while( bytesReaded < scriptLen )
            {
                int num = fifo.read( bytes, (int)bytesReaded, (int)( scriptLen - bytesReaded ) );
                if ( num == -1 )
                    break;

                bytesReaded += num;
            }

            String s = new String( bytes, "UTF-8" );

            String className = "Main";

            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

            JavaFileManager fileManager = new ClassFileManager( compiler.getStandardFileManager( null, null, null ) );

            ArrayList<JavaFileObject> jfiles = new ArrayList<JavaFileObject>();
            jfiles.add( new CharSequenceJavaFileObject( className, s ) );

            CompilationTask task = compiler.getTask( null, fileManager, null, null, null, jfiles );
            if ( task.call() ) {
                Class aClass = fileManager.getClassLoader( null ).loadClass( className );
                Object obj = aClass.newInstance();
                Method method = aClass.getDeclaredMethod( "main", new Class[] { String[].class } );
                String arglist[] = new String[3];
                String taskId = args[3];
                String numTasks = args[4];
                String jobId = args[5];
                arglist[0] = taskId;
                arglist[1] = numTasks;
                arglist[2] = jobId;
                method.invoke( obj, (Object)arglist );
            }
            else {
                errCode = NODE_SCRIPT_EXEC_FAILED;
            }
        }
        catch( Exception e ) {
            errCode = NODE_SCRIPT_EXEC_FAILED;
            System.err.println( e );
        }

        try {
            String fifoName = args[0];
            FileOutputStream fifo = new FileOutputStream( fifoName );
            fifo.write( intToByteArray( errCode ) );
            fifo.close();
        }
        catch( Exception e ) {
            System.err.println( e );
        }
    }

    public static final byte[] intToByteArray(int value) {
        return new byte[] {
            (byte)(value >>> 24),
            (byte)(value >>> 16),
            (byte)(value >>> 8),
            (byte)value};
    }
}

class CharSequenceJavaFileObject extends SimpleJavaFileObject {
    /**
    * CharSequence representing the source code to be compiled
    */
    private CharSequence content;

    /**
    * This constructor will store the source code in the
    * internal "content" variable and register it as a
    * source code, using a URI containing the class full name
    *
    * @param className
    *            name of the public class in the source code
    * @param content
    *            source code to compile
    */
    public CharSequenceJavaFileObject(String className,
        CharSequence content) {
        super(URI.create("string:///" + className.replace('.', '/')
            + Kind.SOURCE.extension), Kind.SOURCE);
        this.content = content;
    }

    /**
    * Answers the CharSequence to be compiled. It will give
    * the source code stored in variable "content"
    */
    @Override
    public CharSequence getCharContent(
        boolean ignoreEncodingErrors) {
        return content;
    }
}

class JavaClassObject extends SimpleJavaFileObject {
    /**
    * Byte code created by the compiler will be stored in this
    * ByteArrayOutputStream so that we can later get the
    * byte array out of it
    * and put it in the memory as an instance of our class.
    */
    protected final ByteArrayOutputStream bos =
        new ByteArrayOutputStream();

    /**
    * Registers the compiled class object under URI
    * containing the class full name
    *
    * @param name
    *            Full name of the compiled class
    * @param kind
    *            Kind of the data. It will be CLASS in our case
    */
    public JavaClassObject(String name, Kind kind) {
        super(URI.create("string:///" + name.replace('.', '/')
            + kind.extension), kind);
    }

    /**
    * Will be used by our file manager to get the byte code that
    * can be put into memory to instantiate our class
    *
    * @return compiled byte code
    */
    public byte[] getBytes() {
        return bos.toByteArray();
    }

    /**
    * Will provide the compiler with an output stream that leads
    * to our byte array. This way the compiler will write everything
    * into the byte array that we will instantiate later
    */
    @Override
    public OutputStream openOutputStream() throws IOException {
        return bos;
    }
}

class ClassFileManager extends ForwardingJavaFileManager {
    /**
    * Instance of JavaClassObject that will store the
    * compiled bytecode of our class
    */
    private JavaClassObject jclassObject;

    /**
    * Will initialize the manager with the specified
    * standard java file manager
    *
    * @param standardManger
    */
    public ClassFileManager(StandardJavaFileManager
        standardManager) {
        super(standardManager);
    }

    /**
    * Will be used by us to get the class loader for our
    * compiled class. It creates an anonymous class
    * extending the SecureClassLoader which uses the
    * byte code created by the compiler and stored in
    * the JavaClassObject, and returns the Class for it
    */
    @Override
    public ClassLoader getClassLoader(Location location) {
        return new SecureClassLoader() {
            @Override
            protected Class<?> findClass(String name)
                throws ClassNotFoundException {
                byte[] b = jclassObject.getBytes();
                return super.defineClass(name, jclassObject
                    .getBytes(), 0, b.length);
            }
        };
    }

    /**
    * Gives the compiler an instance of the JavaClassObject
    * so that the compiler can write the byte code into it.
    */
    @Override
    public JavaFileObject getJavaFileForOutput(Location location,
        String className, Kind kind, FileObject sibling)
            throws IOException {
            jclassObject = new JavaClassObject(className, kind);
        return jclassObject;
    }
}
